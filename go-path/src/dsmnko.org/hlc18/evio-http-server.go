package hlc18

import (
	"fmt"
	"github.com/valyala/fasthttp"
	"log"
	"strconv"
	"strings"
	"tidwall/evio"
)

type Request struct {
	Proto, Method, Path, Query, Head, Body string
}

type Context struct {
	Is  evio.InputStream
	Out [8192]byte
}

func EvioServer(port int, handler func(c evio.Conn, in []byte) (out []byte, action evio.Action)) {

	var events evio.Events

	events.NumLoops = 4
	events.LoadBalance = evio.LeastConnections // todo: ???

	events.Serving = func(srv evio.Server) (action evio.Action) {
		fmt.Printf("%v\thttp server started on port %d (loops: %d)\n", Timenow(), port, srv.NumLoops)
		return
	}

	events.Opened = func(c evio.Conn) (out []byte, opts evio.Options, action evio.Action) {
		c.SetContext(&Context{})
		opts.ReuseInputBuffer = true
		//opts.TCPKeepAlive
		//log.Printf("closed: %s: %s", c.LocalAddr().String(), c.RemoteAddr().String())
		return
	}

	events.Data = handler

	if err := evio.Serve(events, fmt.Sprintf("tcp4://:%d", port)); err != nil { // ?reuseport=true
		log.Fatal(err)
	}
}

func AppendHttpResponse(b []byte, status, headers string, body []byte) []byte {
	b = append(b, "HTTP/1.1 "...)
	b = append(b, status...)
	b = append(b, "\r\nS: e\r\nD: S\r\nC: a\r\n"...)
	if len(body) > 0 {
		b = append(b, "Content-Length: "...)
		b = fasthttp.AppendUint(b, len(body))
		b = append(b, '\r', '\n')
	} else {
		b = append(b, "Content-Length: 0\r\n"...)
	}
	b = append(b, '\r', '\n')
	if len(body) > 0 {
		b = append(b, body...)
	}
	return b
}

func Parsereq(data []byte, req *Request) (leftover []byte, err error) {

	if len(data) < 6 {
		// not enough data
		return data, nil
	}

	sdata := B2s(data) // string(data)
	var i, s int
	var top string
	var clen int
	var q = -1

	// Method, Path, Proto line
	for ; i < len(sdata); i++ {
		if sdata[i] == ' ' {
			req.Method = sdata[s:i]
			for i, s = i+1, i+1; i < len(sdata); i++ {
				if sdata[i] == '?' && q == -1 {
					q = i - s
				} else if sdata[i] == ' ' {
					if q != -1 {
						req.Path = sdata[s : s+q]    //sdata[s:q]
						req.Query = sdata[s+q+1 : i] // req.Path[q+1 : i]
					} else {
						req.Path = sdata[s:i]
					}
					for i, s = i+1, i+1; i < len(sdata); i++ {
						if sdata[i] == '\n' && sdata[i-1] == '\r' {
							req.Proto = sdata[s:i]
							i, s = i+1, i+1
							break
						}
					}
					break
				}
			}
			break
		}
	}
	if req.Proto == "" {
		return data, fmt.Errorf("malformed Request - empty proto, data len %d, sdata: '%s'", len(sdata), sdata)
	}
	top = sdata[:s]
	for ; i < len(sdata); i++ {
		if i > 1 && sdata[i] == '\n' && sdata[i-1] == '\r' {
			line := sdata[s : i-1]
			s = i + 1
			if line == "" {
				req.Head = sdata[len(top)+2 : i+1]
				i++
				if clen > 0 {
					if len(sdata[i:]) < clen {
						break
					}
					req.Body = sdata[i : i+clen]
					i += clen
				}
				return data[i:], nil
			}
			if strings.HasPrefix(line, "Content-Length:") {
				n, err := strconv.ParseInt(strings.TrimSpace(line[len("Content-Length:"):]), 10, 64)
				if err == nil {
					clen = int(n)
				}
			}
		}
	}
	// not enough data
	return data, nil
}
