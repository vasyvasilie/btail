package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"regexp"
	"time"

	"gopkg.in/yaml.v2"
)

const defaultParserName = "parsers.yaml"

type UserParser struct {
	Name   string `yaml:"name"`
	Regexp string `yaml:"regexp"`
	Layout string `yaml:"layout"`
}
type UserParsers struct {
	Parsers []UserParser `yaml:"parsers"`
}

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

func createParsers(secs uint, configPath string) logParsers {
	// parsers here
	ps := logParsers{
		"nginx": logParser{
			r: regexp.MustCompile(`\d{2}\/[a-zA-Z]{3}\/\d{4}:\d{2}:\d{2}:\d{2}\s{1}[\+|\-]\d{4}`),
			l: "02/Jan/2006:15:04:05 -0700",
		},
	}

	data, err := ioutil.ReadFile(configPath)
	if err != nil && configPath != defaultParserName {
		fmt.Println(err)
		os.Exit(1)
	}
	userParsers := UserParsers{}
	err = yaml.Unmarshal([]byte(data), &userParsers)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	for i := range userParsers.Parsers {
		ps[userParsers.Parsers[i].Name] = logParser{
			r: regexp.MustCompile(userParsers.Parsers[i].Regexp),
			l: userParsers.Parsers[i].Layout,
		}
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
				timeInStr, err := time.ParseInLocation(p.l, p.r.FindString(string(splitted[i])), time.Now().Location())
				if err != nil {
					// some parts of buffer cannot be time parsed
					continue
				}
				timeInStr = tryAdoptLogTime(timeInStr)
				if timeInStr.Equal(p.t) || timeInStr.After(p.t) {
					p.t = timeInStr
					break
				}
			}
			toFound := []byte(p.t.Format(p.l))
			toFoundIndex := bytes.Index(buf, toFound)
			if toFoundIndex == -1 {
				break
			}
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

func tryAdoptLogTime(foundedTime time.Time) time.Time {
	currTime := time.Now()
	var appendYear, appendMonth, appendDay int
	if foundedTime.Year() == 0 {
		appendYear = currTime.Year()
	}
	if foundedTime.Month() == 0 {
		appendMonth = int(currTime.Month())
	}
	if foundedTime.Day() == 0 {
		appendMonth = currTime.Day()
	}
	return foundedTime.AddDate(appendYear, appendMonth, appendDay)
}

func (p logParser) findAll(buf []byte) (int, int, int) {
	var l, r, e int
	founds := p.r.FindAll(buf, -1)
	for _, found := range founds {
		timeInStr, err := time.ParseInLocation(p.l, string(found), time.Now().Location())
		if err != nil {
			// never seen this error, so will print
			fmt.Println(err)
			os.Exit(1)
		}
		timeInStr = tryAdoptLogTime(timeInStr)
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
	bufSize := flag.Int("b", 4096, "buffer for read (bytes)") // 16384
	parsersFile := flag.String("p", defaultParserName, "file with dynamic parsers")
	flag.Parse()

	parsers := createParsers(*secs, *parsersFile)
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
	var buf []byte
	for {
		buf = make([]byte, *bufSize)
		_, err = f.ReadAt(buf, int64(offsetCur))
		l, r, e := parser.findAll(buf)
		if e == 0 && r == 0 && l == 0 && offsetCur == 0 {
			fmt.Println("bad parser?")
			os.Exit(1)
		}
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
		} else if l == 1 && offsetCur == 0 {
			parser.printFromOffset(f, offsetCur, *bufSize)
			break
		} else if r == 1 {
			if offsetCur > offsetMax-*bufSize {
				parser.printFromOffset(f, offsetCur, *bufSize)
				break
			}
			if offsetCur == 0 {
				break
			}
			offsetNew = offsetCur + (offsetMax-offsetCur)/2
			if offsetCur == offsetNew {
				offsetNew = 0
			}
		} else if l == 1 {
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
