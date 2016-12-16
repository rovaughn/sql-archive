package main

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	gitignorelib "github.com/monochromegane/go-gitignore"
	"io/ioutil"
	"log"
	"os"
	ospath "path"
	"regexp"
	"runtime/pprof"
	"strings"
	"sync"
	"time"
)

func FormatBytes(n uint64) string {
	f := float32(n)

	if f < 1e3 {
		return fmt.Sprintf("%.1fB", f)
	} else if f < 1e6 {
		return fmt.Sprintf("%.1fkB", f/1e3)
	} else if f < 1e9 {
		return fmt.Sprintf("%.1fMB", f/1e6)
	} else {
		return fmt.Sprintf("%.1fGB", f/1e9)
	}
}

type Fileset struct {
	pos      *regexp.Regexp
	neg      *regexp.Regexp
	toSearch []string
}

// slightly optimizes regexes.  Basically .*$ and ^.* are the same as nothing
// when used in a match.
func Optimize(s string) string {
	return strings.Replace(strings.Replace(s, ".*$", "", -1), "^.*", "", -1)
}

// Semantically, a fileset is the intersection between the set of valid paths
// defined by the given patterns and the files that actually exist on disk.
// A pattern is a string representing a path where an asterisk can expand to
// anything.  A pattern prefixed with a bang negates the pattern.
func NewFileset(patterns []string) (*Fileset, error) {
	var posPieces []string
	var negPieces []string
	var toSearch []string

	for _, pat := range patterns {
		if pat[0] == '!' {
			negPieces = append(negPieces, strings.Replace(regexp.QuoteMeta(pat[1:]), "\\*", ".*", -1))
		} else {
			chunks := strings.Split(pat, "*")
			leftmostSlash := strings.Index(chunks[0], "/")

			if leftmostSlash == -1 && len(chunks) == 1 {
				toSearch = append(toSearch, chunks[0])
			} else if leftmostSlash == -1 && len(chunks) > 1 {
				toSearch = append(toSearch, ".")
			} else {
				toSearch = append(toSearch, chunks[0][:leftmostSlash])
			}

			posPieces = append(posPieces, strings.Replace(regexp.QuoteMeta(pat), "\\*", ".*", -1))
		}
	}

	pos, err := regexp.Compile(Optimize("(^" + strings.Join(posPieces, "$)|(^") + "$)"))
	if err != nil {
		return nil, err
	}

	neg, err := regexp.Compile(Optimize("(^" + strings.Join(negPieces, "$)|(^") + "$)"))
	if err != nil {
		return nil, err
	}

	return &Fileset{
		pos:      pos,
		neg:      neg,
		toSearch: toSearch,
	}, nil
}

func (fs *Fileset) Contains(path string) bool {
	return fs.pos.MatchString(path) && !fs.neg.MatchString(path)
}

type File struct {
	Path string
	Info os.FileInfo
}

func (fs *Fileset) walk(ch chan<- File, wg *sync.WaitGroup, path string, gitignore gitignorelib.IgnoreMatcher) {
	defer wg.Done()
	dir, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer dir.Close()

	files, err := dir.Readdir(0)
	if err != nil {
		panic(err)
	}

	dir.Close()

	newGitignore, err := gitignorelib.NewGitIgnore(ospath.Join(path, ".gitignore"), path)
	if err != nil && !os.IsNotExist(err) {
		panic(err)
	} else if newGitignore != nil {
		gitignore = newGitignore
	}

	for _, file := range files {
		path := ospath.Join(path, file.Name())

		if gitignore != nil && gitignore.Match(path, file.IsDir()) {
			continue
		}

		if file.IsDir() {
			wg.Add(1)
			fs.walk(ch, wg, path, gitignore)
		} else if fs.Contains(path) && (gitignore == nil || !gitignore.Match(path, false)) {
			ch <- File{
				Path: path,
				Info: file,
			}
		}
	}
}

func (fs *Fileset) Files() <-chan File {
	ch := make(chan File)

	var wg sync.WaitGroup

	wg.Add(len(fs.toSearch))
	for _, path := range fs.toSearch {
		go fs.walk(ch, &wg, path, nil)
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	return ch
}

func (file *File) Read() ([]byte, error) {
	if file.Info.IsDir() || file.Info.Mode()&os.ModeSymlink != 0 {
		link, err := os.Readlink(file.Path)
		return []byte(link), err
	} else {
		return ioutil.ReadFile(file.Path)
	}
}

type DBLock struct {
	lock sync.Mutex
	db   *sql.DB
	tx   *sql.Tx
}

func (dbl *DBLock) Lock() *sql.Tx {
	dbl.lock.Lock()
	return dbl.tx
}

func (dbl *DBLock) Unlock() {
	dbl.lock.Unlock()
}

func (dbl *DBLock) Commit() error {
	if dbl != nil {
		dbl.tx.Commit()
		dbl.db.Close()
	}
	return nil
}

func (dbl *DBLock) Rollback() error {
	if dbl != nil {
		dbl.tx.Rollback()
		dbl.db.Close()
	}
	return nil
}

func OpenDB(name string) (*DBLock, error) {
	db, err := sql.Open("sqlite3", name)
	if err != nil {
		return nil, err
	}

	var schemaVersion int
	if err := db.QueryRow("PRAGMA schema_version").Scan(&schemaVersion); err != nil {
		return nil, err
	}

	if schemaVersion == 0 {
		if _, err := db.Exec(`
			CREATE TABLE file (
				path    TEXT UNIQUE NOT NULL,
				modtime TIMESTAMP NOT NULL,
				mode    INTEGER NOT NULL,
				data    BLOB NULL
			)
		`); err != nil {
			return nil, err
		}
	}

	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}

	return &DBLock{
		db: db,
		tx: tx,
	}, nil
}

func main() {
	dest := flag.String("dest", "", "Database file to synchronize with filesystem")
	newerThanVar := flag.Int("newer-than", 0, "Only include changes newer than this timestamp.")
	maxSize := flag.Int("max-size", 1000*1000, "Maximum file size to back up")
	cpuprofile := flag.String("cpuprofile", "", "CPU profile output")
	flag.Parse()

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			panic(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	log.Println("dest", *dest)

	if *dest == "" {
		log.Printf("-dest required")
		os.Exit(1)
	}

	newerThan := time.Unix(int64(*newerThanVar), 0)

	fileset, err := NewFileset(flag.Args())
	if err != nil {
		panic(err)
	}

	db, err := OpenDB(*dest)
	if err != nil {
		panic(err)
	}
	defer db.Rollback()

	insertQuery, err := db.tx.Prepare(`INSERT INTO file (path, modtime, mode, data) VALUES (?, ?, ?, ?)`)
	if err != nil {
		panic(err)
	}

	handled := make(map[string]bool)

	var wg sync.WaitGroup

	for file := range fileset.Files() {
		if handled[file.Path] {
			continue
		}

		if file.Info.Size() > int64(*maxSize) {
			panic(fmt.Sprintf("%s is bigger than max size (%s > %s)", file.Path, FormatBytes(uint64(file.Info.Size())), FormatBytes(uint64(*maxSize))))
		}

		if !file.Info.ModTime().After(newerThan) {
			continue
		}

		wg.Add(1)
		go func(file File) {
			defer wg.Done()

			data, err := file.Read()
			if err != nil {
				log.Printf("Reading %s: %s", file.Path, err)
				return
			}

			db.Lock()
			defer db.Unlock()

			if _, err := insertQuery.Exec(file.Path, file.Info.ModTime(), file.Info.Mode(), data); err != nil {
				panic(err)
			}
			log.Printf("Added %s", file.Path)
		}(file)
	}

	wg.Wait()
	if err := db.Commit(); err != nil {
		panic(err)
	}
}
