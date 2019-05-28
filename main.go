package main

import (
	"flag"
	"github.com/leenzhu/goxlog"
	"os"
	"bufio"
	"path/filepath"
	"strings"
	"io"
	"crypto/md5"
	"fmt"
	"github.com/rwcarlsen/goexif/exif"
	"errors"
	//"github.com/rwcarlsen/goexif/mknote"
)

type optContex struct {
	mode string
	outputDir string
	inputDir string
	md5File string
	dupDir string
	pathMd5 map[string]string
	md5Path map[string]string
}
func main() {
	var ctx optContex
	flag.StringVar(&ctx.outputDir, "o", "", "output directory")
	flag.StringVar(&ctx.inputDir, "i", "", "input directory")
	flag.StringVar(&ctx.dupDir, "d", "", "input directory")
	flag.StringVar(&ctx.md5File, "m", "", "md5 file of output directory")
	flag.StringVar(&ctx.mode, "M", "check", "work mode: check|sync")
	flag.Parse()

	if !validOption(&ctx) {
		return
	}

	switch ctx.mode {
	case "check":
		fileCheck(&ctx)
	case "sync":
		fileCheck(&ctx)
		fileSync(&ctx)
	default:
	}

}

func finishMd5File(file *os.File, w *bufio.Writer, path string) {
	w.Flush()
	file.Close()
	os.Rename(path+".tmp", path)
}

func openMd5File(path string) (*os.File, *bufio.Writer, error) {
	file, err := os.Create(path+".tmp")
	if err != nil {
		xlog.Errorf("open %s failed:%v\n", path, err)
		return nil, nil, err
	}
	w := bufio.NewWriter(file)

	return file, w, nil
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

func getOutput(path, dst string) string {
	fullName := filepath.Base(path)
	dt, err := getExif(path)
	if err != nil {
		return ""
	}

	return getUniqueName(filepath.Join(dst, dt, fullName)) 
}

func getUniqueName(path string) string {
	var destFile string
	fullName := filepath.Base(path)
	dir := filepath.Dir(path)
	ext := filepath.Ext(fullName)
	name := strings.TrimSuffix(fullName, ext)

	for idx := 1;;idx++ {
		destFile = filepath.Join(dir, fullName)
		if !exists(destFile) {
			break
		}

		fullName = fmt.Sprintf("%s_%d%s", name, idx, ext)
		xlog.Debugf("warning: %s conflict, new name:%s\n", destFile, fullName)
	}

	return destFile
}

func moveToDup(path, dupDir string) error {
	fullName := filepath.Base(path)
	
	destFile := getUniqueName(filepath.Join(dupDir, fullName))
	xlog.Infof("MOVDUP %s -> %s\n", path, destFile)
	return os.Rename(path, destFile)
}

func moveToDate(path, dest string) error{
	dst := getOutput(path, dest)
	if dst == "" {
		xlog.Errorf("get dest name failed\n")
		return errors.New("get dest name failed\n")
	}
	err := os.Rename(path, dst)
	if err != nil {
		xlog.Errorf("move file failed:%v\n", err)
		return err
	}
	xlog.Infof("SYNC %s -> %s\n", path, dst)
	return nil
}

func fileSync(ctx *optContex) {
	fileIdx := 0
	filepath.Walk(ctx.inputDir, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			xlog.Errorf("%s error with %v\n", err)
			return nil
		}
		if f.IsDir() {
			return nil
		}
		fileIdx += 1
		xlog.Debugf("%06d SYNC %s\n", fileIdx, path);

		md5sum := md5Sum(path)

		if oldPath, ok := ctx.md5Path[md5sum]; ok {
			xlog.Infof("DUP %s|%s\n", oldPath, path)
			moveToDup(path, ctx.dupDir)
		} else {
			xlog.Debugf("md5 count:%d, md5=%s\n", len(ctx.md5Path), md5sum)
			os.Exit(0)
			if err := moveToDate(path, ctx.outputDir); err != nil {
				xlog.Errorf("Sync %s failed:%v\n", path, err)
			} else {
				xlog.Infof("SYNC %s OK\n", path)
			}
		}
		return nil
	})
}

func fileCheck(ctx *optContex) {
	// step 1. load md5.sum records
	xlog.Debugf("STEP 1 load md5.sum\n")
	ctx.pathMd5 = loadPathMd5(ctx.md5File)

	// step 2. remove all deleted files records
	xlog.Debugf("STEP 2 DEL records of non-exists files\n")
	for path, _ := range ctx.pathMd5 {
		if !exists(path) {
			xlog.Infof("DEL %s\n", path)
			delete(ctx.pathMd5, path)
		}
	}

	// step 3. build md5Path for unique file check
	xlog.Debugf("STEP 3 build revers md5 Map ---\n")
	ctx.md5Path = reversMap(ctx.pathMd5)
	xlog.Debugf("len(ctx.md5Path)=%d\n", len(ctx.md5Path));
	// step 4. open temp md5sum file
	xlog.Debugf("STEP 4 open md5 file for writing\n")
	file, w, err := openMd5File(ctx.md5File)
	if err != nil {
		return
	}

	// step 5. traval directory process each file
	xlog.Debugf("STEP 5 process each file\n")
	fileIdx := 0
	filepath.Walk(ctx.outputDir, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			xlog.Errorf("%s error with %v\n", err)
			return nil
		}
		if f.IsDir() {
			return nil
		}
		fileIdx += 1
		xlog.Debugf("%06d Processing %s\n", fileIdx, path)

		if oldMd5sum, ok := ctx.pathMd5[path]; !ok { // this is a new file(path)
			md5sum := md5Sum(path)
			xlog.Debugf("md5sum new file %s\n", path)

			// if this file has the same md5 with another, ignore this file
			if oldPath, ok := ctx.md5Path[md5sum]; ok {
				xlog.Infof("DUP %s|%s\n", oldPath, path)
				moveToDup(path, ctx.dupDir)
			} else {
				xlog.Infof("NEW %s\n", path)
				ctx.md5Path[md5sum] = path
				ctx.pathMd5[path] = md5sum

				w.WriteString(fmt.Sprintf("%s|%s\n", md5sum, path))
			}
		} else {
			xlog.Debugf("SKIP %s\n", path)
			w.WriteString(fmt.Sprintf("%s|%s\n", oldMd5sum, path))
		}

		return nil
	})

	// step 6. Finish processing
	xlog.Debugf("STEP 6 Finish processing\n")
	finishMd5File(file, w, ctx.md5File)
	xlog.Debugf("check done!\n")
}

func exists(path string) bool{
	_, err := os.Stat(path)
	if err == nil { return true}
	if os.IsNotExist(err) { return false}
	return true
}

func md5Sum(path string) string{
	f, err := os.Open(path)
	if err != nil {
		xlog.Errorf("%v\n", err)
		return ""
	}
	defer f.Close()

	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		xlog.Errorf("%v\n", err)
		return ""
	}

	return fmt.Sprintf("%x", h.Sum(nil))
}

func reversMap(ori map[string]string) map[string]string {
	newMap :=make(map[string]string)
	
	for k, v := range ori {
		newMap[v] = k
	}

	return newMap
}

func loadPathMd5(path string) map[string]string{
	pathMd5 := loadMd5File(path)
	pathMd5Tmp := loadMd5File(path+".tmp")

	for k, v := range pathMd5Tmp {
		pathMd5[k] = v;
	}

	return pathMd5
}
// format: xxxxxxxx|/path/to/file
func loadMd5File(path string) map[string]string {
	pathMd5 := make(map[string]string)

	file, err := os.Open(path)
	if err != nil {
		xlog.Warnf("open md5 file failed: %s\n", err)
		return pathMd5
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)

	lineno := 1
	for scanner.Scan() {
		line := scanner.Text()
		tokens := strings.Split(line, "|")
		if len(tokens) == 2 {
			md5 := tokens[0]
			path := tokens[1]
			pathMd5[path] = md5
		} else {
			xlog.Debugf("invalid line(%d) in %s\n", lineno, path)
		}
		lineno = lineno + 1
	}

	return pathMd5
}

func validOption(ctx *optContex) bool {
	if !hasFlag(ctx.outputDir) {
		xlog.Errorf("ouput option must be provided\n")
		return false
	}

	if !hasFlag(ctx.inputDir) {
		xlog.Errorf("input option must be provided\n")
		return false
	}

	if !hasFlag(ctx.md5File) {
		xlog.Errorf("md5 option must be provided\n")
		return false
	}

	if !hasFlag(ctx.dupDir) {
		xlog.Errorf("md5 option must be provided\n")
		return false
	}
	return true
}

func hasFlag(f string) bool {
	if f == "" {
		return false
	}

	return true
}


