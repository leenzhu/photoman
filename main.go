package main

import (
	"os"
	"path/filepath"
	"github.com/rwcarlsen/goexif/exif"
	"github.com/rwcarlsen/goexif/mknote"
	"github.com/leenzhu/goxlog"
	"strings"
	"fmt"
	"flag"
)

var (
	dst string
	src string
)
func main() {
	// Optionally register camera makenote data parsing - currently Nikon and
	// Canon are supported.
	flag.StringVar(&dst, "d", "dst", "dest dir")
	flag.StringVar(&src, "s", "src", "source dir")
	flag.Parse()

	exif.RegisterParsers(mknote.All...)


	filepath.Walk(src, func(path string, f os.FileInfo, err error) error{
		if err != nil {
			xlog.Errorf("%s error with %v\n", err)
			return nil
		}
		if f.IsDir() {
			xlog.Infof("skip directory %s", path)
			return nil
		}


		fullName := f.Name()
		ext := filepath.Ext(fullName)
		name := strings.TrimSuffix(fullName, ext)
		xlog.Debugf("processing %s, name=%s %s\n", path, name, ext);
		dt, err := getExif(path)
		if err != nil {
			return nil
		}

		var dstFile string
		for idx := 1;;idx++ {
			dstFile = filepath.Join(dst, dt, fullName)
			if !exists(dstFile) {
				break
			}

			fullName = fmt.Sprintf("%s_%d%s", name, idx, ext)
			xlog.Debugf("warning: %s conflict, new name:%s\n", dst, fullName)
		}
		
		xlog.Infof("move file %s -> %s\n", path, dstFile)
		err = os.Rename(path, dstFile)
		if err != nil {
			xlog.Errorf("move file failed:%v\n", err)
		}
		return nil
	})

}

func exists(path string) bool{
	_, err := os.Stat(path)
	if err == nil { return true}
	if os.IsNotExist(err) { return false}
	return true
}

func getExif(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		xlog.Errorf("open %s failed:%v\n", path, err)
		return "", err
	}
	defer f.Close()

	x, err := exif.Decode(f)
	if err != nil {
		xlog.Errorf("decode %s failed:%v\n", path, err)
		return "", err
	}

	d, err := x.DateTime()
	if err != nil {
		xlog.Infof("original date not found in %s, err=%v\n", path, err)
		return "", err
	}

	dt := d.Format("2006-01")
	xlog.Debugf("OriginalDate: %s %s\n", dt, path)
	return dt, nil
}
