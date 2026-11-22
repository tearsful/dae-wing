/*
 * SPDX-License-Identifier: AGPL-3.0-only
 * Copyright (c) 2026, tearsful
 */

package geoupdate

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/daeuniverse/dae-wing/db"
	"github.com/daeuniverse/dae-wing/graphql/service/config"
	"github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"
)

// DefaultCron is Thursday 10:00:00 local time (seconds-enabled cron).
// Empty cron disables the job. Users can turn it off or change frequency in the panel.
const DefaultCron = "0 0 10 * * 4"

// DefaultGeoDir is the hard-coded TARGET_DIR in docker/glider/update_geo.sh.
const DefaultGeoDir = "/usr/local/share/daed"

// DefaultTmpDir is the hard-coded TMP_DIR in docker/glider/update_geo.sh.
const DefaultTmpDir = "/tmp/geo_update"

var cronParser = cron.NewParser(
	cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
)

type geofileEntry struct {
	FileName string
	URLs     []string
}

// defaultAllowlist follows docker/glider/update_geo.sh (+ custom.dat shipped in the image).
func defaultAllowlist() map[string]geofileEntry {
	return map[string]geofileEntry{
		"geoip.dat": {
			FileName: "geoip.dat",
			URLs: []string{
				"https://github.com/Loyalsoldier/v2ray-rules-dat/releases/latest/download/geoip.dat",
			},
		},
		"geosite.dat": {
			FileName: "geosite.dat",
			URLs: []string{
				"https://github.com/Loyalsoldier/v2ray-rules-dat/releases/latest/download/geosite.dat",
			},
		},
		"custom.dat": {
			FileName: "custom.dat",
			URLs: []string{
				"https://github.com/Loyalsoldier/domain-list-custom/releases/latest/download/geosite.dat",
			},
		},
	}
}

// GeoDir returns the install directory used by this project.
// Must match docker/glider/update_geo.sh TARGET_DIR=/usr/local/share/daed.
// DAE_LOCATION_ASSET may override only when explicitly set (Dockerfile sets the same path).
func GeoDir() string {
	if v := strings.TrimSpace(os.Getenv("DAE_LOCATION_ASSET")); v != "" {
		return filepath.Clean(v)
	}
	return DefaultGeoDir
}

func ValidateCron(expr string) error {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil
	}
	if _, err := cronParser.Parse(expr); err != nil {
		return fmt.Errorf("invalid geoUpdateCron %q: %w", expr, err)
	}
	return nil
}

func GetCron() (string, error) {
	var sys db.System
	if err := db.DB(context.TODO()).Model(&db.System{}).FirstOrCreate(&sys).Error; err != nil {
		return "", err
	}
	return strings.TrimSpace(sys.GeoUpdateCron), nil
}

func SetCron(expr string) error {
	expr = strings.TrimSpace(expr)
	if err := ValidateCron(expr); err != nil {
		return err
	}
	var sys db.System
	if err := db.DB(context.TODO()).Model(&db.System{}).FirstOrCreate(&sys).Error; err != nil {
		return err
	}
	return db.DB(context.TODO()).Model(&sys).Update("geo_update_cron", expr).Error
}

// UpdateGeofile mirrors docker/glider/update_geo.sh:
// download into /tmp/geo_update → verify → backup → mv into /usr/local/share/daed → reload.
func UpdateGeofile(fileName string) error {
	allow := defaultAllowlist()
	if fileName != "" {
		if _, ok := allow[fileName]; !ok {
			return fmt.Errorf("invalid geofile name: %q (allowed: geoip.dat, geosite.dat, custom.dat)", fileName)
		}
	}

	destDir := GeoDir()
	tmpDir := DefaultTmpDir

	if err := os.MkdirAll(destDir, 0755); err != nil {
		return fmt.Errorf("create geo dir %s: %w", destDir, err)
	}
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		return fmt.Errorf("create temp dir %s: %w", tmpDir, err)
	}
	defer func() {
		_ = os.RemoveAll(tmpDir)
	}()

	// Clean stale temps (same as update_geo.sh).
	for _, name := range []string{"geoip.dat", "geosite.dat", "custom.dat"} {
		_ = os.Remove(filepath.Join(tmpDir, name))
	}

	keys := []string{"geosite.dat", "geoip.dat", "custom.dat"}
	if fileName != "" {
		keys = []string{fileName}
	}

	logrus.Infof("Geofile update: TMP=%s TARGET=%s", tmpDir, destDir)

	type staged struct {
		name    string
		tmpPath string
		dest    string
	}
	var files []staged

	for _, key := range keys {
		entry := allow[key]
		tmpPath := filepath.Join(tmpDir, entry.FileName)
		if err := downloadToFile(entry.URLs, tmpPath); err != nil {
			return fmt.Errorf("download %s: %w", entry.FileName, err)
		}
		fi, err := os.Stat(tmpPath)
		if err != nil || fi.Size() <= 0 {
			return fmt.Errorf("downloaded %s missing or empty", entry.FileName)
		}
		files = append(files, staged{
			name:    entry.FileName,
			tmpPath: tmpPath,
			dest:    filepath.Join(destDir, entry.FileName),
		})
		logrus.Infof("Geofile downloaded OK: %s (%d bytes)", entry.FileName, fi.Size())
	}

	// Backup existing live files, then mv from /tmp into TARGET_DIR.
	for _, f := range files {
		if _, err := os.Stat(f.dest); err == nil {
			bak := filepath.Join(os.TempDir(), fmt.Sprintf("%s.backup.%s", f.name, time.Now().Format("20060102_150405")))
			if err := copyFile(f.dest, bak); err != nil {
				logrus.Warnf("backup %s failed: %v", f.dest, err)
			} else {
				logrus.Infof("backed up %s → %s", f.dest, bak)
			}
		}
		if err := os.Rename(f.tmpPath, f.dest); err != nil {
			// Cross-device: copy then remove temp.
			if err2 := copyFile(f.tmpPath, f.dest); err2 != nil {
				return fmt.Errorf("install %s → %s: rename=%v copy=%v", f.name, f.dest, err, err2)
			}
			_ = os.Remove(f.tmpPath)
		}
		_ = os.Chmod(f.dest, 0644)
		fi, err := os.Stat(f.dest)
		if err != nil || fi.Size() <= 0 {
			return fmt.Errorf("installed %s missing or empty", f.dest)
		}
		logrus.Infof("Geofile installed: %s (%d bytes)", f.dest, fi.Size())
	}

	return reloadService()
}

func downloadToFile(urls []string, tmpPath string) error {
	client := &http.Client{Timeout: 5 * time.Minute}
	var lastErr error
	for _, url := range urls {
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			lastErr = err
			continue
		}
		req.Header.Set("User-Agent", "daed-geoupdate/1.0")
		resp, err := client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("%s: %w", url, err)
			logrus.Warn(lastErr)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			lastErr = fmt.Errorf("%s: HTTP %d", url, resp.StatusCode)
			logrus.Warn(lastErr)
			continue
		}
		f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
		if err != nil {
			resp.Body.Close()
			return err
		}
		n, copyErr := io.Copy(f, resp.Body)
		closeErr := f.Close()
		resp.Body.Close()
		if copyErr != nil || closeErr != nil || n <= 0 {
			os.Remove(tmpPath)
			if copyErr != nil {
				lastErr = copyErr
			} else if closeErr != nil {
				lastErr = closeErr
			} else {
				lastErr = fmt.Errorf("%s: empty body", url)
			}
			continue
		}
		return nil
	}
	if lastErr != nil {
		return lastErr
	}
	return fmt.Errorf("no URL succeeded for %s", filepath.Base(tmpPath))
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(out, in)
	closeErr := out.Close()
	if copyErr != nil {
		return copyErr
	}
	return closeErr
}

func reloadService() error {
	var sys db.System
	if err := db.DB(context.TODO()).Model(&db.System{}).FirstOrCreate(&sys).Error; err != nil {
		return err
	}
	if !sys.Running {
		logrus.Infof("Geofile installed under %s; daed is not running, skip reload", GeoDir())
		return nil
	}
	tx := db.BeginTx(context.TODO())
	if _, err := config.Run(tx, false); err != nil {
		tx.Rollback()
		return fmt.Errorf("geofile installed but service reload failed: %w", err)
	}
	tx.Commit()
	logrus.Info("Geofile update applied; daed service reloaded")
	return nil
}

var (
	mu    sync.Mutex
	c     *cron.Cron
	entry cron.EntryID
)

func Start() {
	mu.Lock()
	defer mu.Unlock()
	if c == nil {
		c = cron.New(
			cron.WithLocation(time.Local),
			cron.WithSeconds(),
			cron.WithChain(cron.SkipIfStillRunning(cron.DefaultLogger)),
		)
		c.Start()
	}
	// DB default for new System rows is Thursday 10:00 (DefaultCron).
	// Empty cron means the user disabled scheduled updates — do not re-seed.
	if err := rescheduleLocked(); err != nil {
		logrus.Warnf("geoupdate schedule: %v", err)
	}
}

func Reschedule() error {
	mu.Lock()
	defer mu.Unlock()
	return rescheduleLocked()
}

func rescheduleLocked() error {
	if c == nil {
		return nil
	}
	if entry != 0 {
		c.Remove(entry)
		entry = 0
	}
	expr, err := GetCron()
	if err != nil {
		return err
	}
	expr = strings.TrimSpace(expr)
	if expr == "" {
		logrus.Info("Geofile scheduled update disabled")
		return nil
	}
	if err := ValidateCron(expr); err != nil {
		return err
	}
	id, err := c.AddFunc(expr, func() {
		logrus.Info("Scheduled Geofile update starting…")
		if err := UpdateGeofile(""); err != nil {
			logrus.Warnf("Scheduled Geofile update failed: %v", err)
			return
		}
		logrus.Info("Scheduled Geofile update completed")
	})
	if err != nil {
		return err
	}
	entry = id
	logrus.Infof("Geofile scheduled update enabled: %s (TARGET=%s)", expr, GeoDir())
	return nil
}
