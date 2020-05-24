package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"time"
)

type logParser struct {
	r *regexp.Regexp
	l string
	t time.Time
}

type logParsers map[string]logParser

func listParsers(parsers logParsers) {
	fmt.Println("available formats:")
	for i := range parsers {
		fmt.Println("name:", i, "format:", parsers[i].l)
	}

}

func createParsers(secs uint) logParsers {
	// parsers here
	ps := logParsers{
		"nginx": logParser{
			r: regexp.MustCompile(`\d{2}\/[a-zA-Z]{3}\/\d{4}:\d{2}:\d{2}:\d{2}\ \+\d{4}`),
			l: "02/Jan/2006:15:04:05 -0700",
		},
	}

	// add after to all parsers
	now := time.Now()
	checkAfter := now.Add(time.Duration(-secs) * time.Second)
	for name := range ps {
		p := ps[name]
		p.t = checkAfter
		ps[name] = p
	}
	return ps
}

func getFileSize(f *os.File) int {
	s, err := f.Stat()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	return int(s.Size())
}

func (p logParser) printFromOffset(f *os.File, currOffset int, bufSize int) {
	offsetMax := getFileSize(f)
	nl := []byte("\n")
	buf := make([]byte, bufSize)
	init := true
	var err error
	for {
		if currOffset > offsetMax {
			break
		}
		if currOffset+bufSize > offsetMax {
			buf = make([]byte, offsetMax-currOffset)
		}
		_, err = f.ReadAt(buf, int64(currOffset))
		if err != nil {
			if err == io.EOF {
				fmt.Println()
				break
			}
			fmt.Println(err)
			os.Exit(1)
		}
		if init {
			splitted := bytes.Split(buf, nl)
			for i := 0; i < len(splitted); i++ {
				if len(splitted[i]) == 0 {
					continue
				}
				timeInStr, err := time.Parse(p.l, p.r.FindString(string(splitted[i])))
				if err != nil {
					// some parts of buffer cannot be time parsed
					continue
				}
				if timeInStr.Equal(p.t) || timeInStr.After(p.t) {
					p.t = timeInStr
					break
				}
			}
			toFound := []byte(p.t.Format(p.l))
			toFoundIndex := bytes.Index(buf, toFound)
			lastnl := bytes.LastIndex(buf[:toFoundIndex], nl)
			fmt.Print(string(buf[lastnl+1:]))
			currOffset = currOffset + bufSize
			init = false
			continue
		}
		fmt.Print(string(buf))
		currOffset = currOffset + bufSize
	}
}

func (p logParser) findAll(buf []byte) (int, int, int) {
	var l, r, e int
	founds := p.r.FindAll(buf, -1)
	for _, found := range founds {
		timeInStr, _ := time.Parse(p.l, string(found))
		if timeInStr.After(p.t) {
			l = 1
		}
		if timeInStr.Before(p.t) {
			r = 1
		}
		if timeInStr.Equal(p.t) {
			e = 1
		}
	}
	return l, r, e
}

func main() {
	secs := flag.Uint("n", 300, "seconds")
	lfile := flag.String("f", "access.log", "path to file")
	ltype := flag.String("t", "nginx", "log format")
	bufSize := flag.Int("b", 16384, "buffer for read (bytes)") // 16384
	flag.Parse()

	parsers := createParsers(*secs)
	parser, ok := parsers[*ltype]
	if !ok {
		listParsers(parsers)
		os.Exit(1)
	}

	f, err := os.Open(*lfile)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer f.Close()

	var offsetNew int
	offsetMax := getFileSize(f)
	offsetCur := int(offsetMax / 2)
	buf := make([]byte, *bufSize)

	for {
		_, err = f.ReadAt(buf, int64(offsetCur))
		l, r, e := parser.findAll(buf)
		if e == 1 && r == 1 && l == 1 {
			parser.printFromOffset(f, offsetCur, *bufSize)
			break
		} else if e == 1 && l == 1 && offsetCur == 0 {
			parser.printFromOffset(f, offsetCur, *bufSize)
			break
		} else if e == 1 && r == 1 && err != nil && err == io.EOF {
			// EOF and found
			parser.printFromOffset(f, offsetCur, *bufSize)
			break
		} else if l == 1 && r == 1 && offsetCur == 0 {
			parser.printFromOffset(f, offsetCur, *bufSize)
			break
		} else if r == 1 {
			if offsetCur == 0 {
				break
			}
			offsetNew = offsetCur + (offsetMax-offsetCur)/2
			if offsetCur == offsetNew {
				offsetNew = 0
			}
		} else if l == 1 {
			if offsetCur == 0 {
				break
			}
			offsetNew = offsetCur - (offsetMax-offsetCur)/2
			if offsetCur == offsetNew {
				offsetNew = 0
			}
			offsetMax = offsetCur
		} else if err == io.EOF {
			break
		}
		offsetCur = offsetNew
	}
}
