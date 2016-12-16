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

	log.Printf("pos %q neg %q", pos, neg)

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

func HandleExisting(db *sql.DB, dbmx *sync.Mutex, handled map[string]bool, fileset *Fileset) {
	dbmx.Lock()
	defer dbmx.Unlock()

	tx, err := db.Begin()
	if err != nil {
		panic(err)
	}
	defer tx.Rollback()

	rows, err := tx.Query("SELECT path, modtime FROM file")
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	var toDelete []string
	var toUpdate []File

	for rows.Next() {
		var path string
		var modtime time.Time
		if err := rows.Scan(&path, &modtime); err != nil {
			panic(err)
		}

		if !fileset.Contains(path) {
			toDelete = append(toDelete, path)
			continue
		}

		fi, err := os.Lstat(path)
		if os.IsNotExist(err) {
			toDelete = append(toDelete, path)
		} else if err != nil {
			panic(err)
		} else if modtime.Before(fi.ModTime()) {
			toUpdate = append(toUpdate, File{
				Path: path,
				Info: fi,
			})
		}

		handled[path] = true
	}

	rows.Close()

	for _, path := range toDelete {
		log.Printf("Dropping %s", path)
		if _, err := tx.Exec(`DELETE FROM file WHERE path = ?`, path); err != nil {
			panic(err)
		}
	}

	for _, file := range toUpdate {
		data, err := file.Read()
		if err != nil {
			panic(err)
		}

		log.Printf("Updating %s", file.Path)
		if _, err := tx.Exec(`UPDATE file SET modtime = ?, mode = ?, data = ? WHERE path = ?`, file.Info.ModTime(), file.Info.Mode(), data, file.Path); err != nil {
			panic(err)
		}
	}

	tx.Commit()
}

func (file *File) Read() ([]byte, error) {
	if file.Info.IsDir() || file.Info.Mode()&os.ModeSymlink != 0 {
		link, err := os.Readlink(file.Path)
		return []byte(link), err
	} else {
		return ioutil.ReadFile(file.Path)
	}
}

func main() {
	dest := flag.String("dest", "", "Destination file")
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

	if *dest == "" {
		log.Printf("-dest required")
		os.Exit(1)
	}

	fileset, err := NewFileset(flag.Args())
	if err != nil {
		panic(err)
	}

	db, err := sql.Open("sqlite3", *dest)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	var schemaVersion int

	if err := db.QueryRow("PRAGMA schema_version").Scan(&schemaVersion); err != nil {
		panic(err)
	}

	if schemaVersion == 0 {
		if _, err := db.Exec(`
			CREATE TABLE file (
				path TEXT UNIQUE NOT NULL,
				modtime TIMESTAMP NOT NULL,
				mode INTEGER NOT NULL,
				data BLOB
			)
		`); err != nil {
			panic(err)
		}
	}

	var dbmx sync.Mutex

	handled := make(map[string]bool)
	HandleExisting(db, &dbmx, handled, fileset)

	var wg sync.WaitGroup

	tx, err := db.Begin()
	if err != nil {
		panic(err)
	}
	defer tx.Rollback()

	insertQuery, err := tx.Prepare(`INSERT INTO file (path, modtime, mode, data) VALUES (?, ?, ?, ?)`)
	if err != nil {
		panic(err)
	}
	defer insertQuery.Close()

	for file := range fileset.Files() {
		if handled[file.Path] {
			continue
		}

		wg.Add(1)
		go func(file File) {
			defer wg.Done()
			dbmx.Lock()
			defer dbmx.Unlock()

			if file.Info.Size() > int64(*maxSize) {
				panic(fmt.Sprintf("%s is bigger than max size (%s > %s)", file.Path, FormatBytes(uint64(file.Info.Size())), FormatBytes(uint64(*maxSize))))
			}

			data, err := file.Read()
			if err != nil {
				log.Printf("Reading %s: %s", file.Path, err)
				return
			}

			log.Printf("Adding %s", file.Path)
			if _, err := insertQuery.Exec(file.Path, file.Info.ModTime(), file.Info.Mode(), data); err != nil {
				panic(err)
			}
		}(file)
	}

	wg.Wait()
	tx.Commit()
}
