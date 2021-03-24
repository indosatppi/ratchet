package util

import (
	"io/ioutil"
	"os"
	"path"
	"regexp"
	"strings"
)

type FileClient interface{
	ReadDir(dirname string)([]os.FileInfo, error)
	Stat(name string)(os.FileInfo, error)
}


func NewLocalFileClient() FileClient {
	return localFileClient{}
}

type localFileClient struct {}
func (c localFileClient) ReadDir(dirname string)([]os.FileInfo, error) {
	return ioutil.ReadDir(dirname)
}

func (c localFileClient) Stat(name string)(os.FileInfo, error) {
	return os.Stat(name)
}

// TODO: not really sure checking for a leading \ is the way to test for
// escapedness.
func hasGlob(fragment string) bool {
	match, _ := regexp.MatchString(`([^\\]|^)[[*?]`, fragment)
	return match
}

// expandGlobs recursively expands globs in a filepath. It assumes the paths
// are already cleaned and normalized (ie, absolute).
func expandGlobs(client FileClient, globbedPath string) ([]string, error) {
	parts := strings.Split(globbedPath, "/")[1:]
	var res []string
	var splitAt int

	for splitAt = range parts {
		if hasGlob(parts[splitAt]) {
			break
		}
	}

	var base, glob, next, remainder string
	base = "/" + path.Join(parts[:splitAt]...)
	glob = parts[splitAt]

	if len(parts) > splitAt+1 {
		next = parts[splitAt+1]
		remainder = path.Join(parts[splitAt+2:]...)
	} else {
		next = ""
		remainder = ""
	}

	list, err := client.ReadDir(base)
	if err != nil {
		return nil, err
	}

	for _, fi := range list {
		match, _ := path.Match(glob, fi.Name())
		if !match {
			continue
		}

		newPath := path.Join(base, fi.Name(), next, remainder)
		if hasGlob(newPath) {
			if fi.IsDir() {
				children, err := expandGlobs(client, newPath)
				if err != nil {
					return nil, err
				}

				res = append(res, children...)
			}
		} else {
			_, err := client.Stat(newPath)
			if os.IsNotExist(err) {
				continue
			} else if err != nil {
				return nil, err
			}

			res = append(res, newPath)
		}
	}

	return res, nil
}

//ExpandPaths paths should be an absolute path
func ExpandPaths(client FileClient, paths []string) ([]string, error) {
	var res []string

	for _, p := range paths {
		if hasGlob(p) {
			expanded, err := expandGlobs(client, p)
			if err != nil {
				return nil, err
			} else if len(expanded) == 0 {
				// Fake a PathError for consistency.
				return nil, &os.PathError{"stat", p, os.ErrNotExist}
			}

			res = append(res, expanded...)
		} else {
			res = append(res, p)
		}
	}

	return res, nil
}