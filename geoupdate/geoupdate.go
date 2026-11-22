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

// DefaultGeoDir matches docker/glider (ENV DAE_LOCATION_ASSET=/usr/local/share/daed).
const DefaultGeoDir = "/usr/local/share/daed"

var cronParser = cron.NewParser(
	cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
)

type geofileEntry struct {
	FileName string
	URLs     []string
}

// defaultAllowlist mirrors docker/glider/update_geo.sh sources used by this project.
func defaultAllowlist() map[string]geofileEntry {
	return map[string]geofileEntry{
		"geoip.dat": {
			FileName: "geoip.dat",
			URLs: []string{
				"https://github.com/Loyalsoldier/v2ray-rules-dat/releases/latest/download/geoip.dat",
				"https://cdn.jsdelivr.net/gh/DustinWin/ruleset_geodata@mihomo-geodata/geoip-all.dat",
			},
		},
		"geosite.dat": {
			FileName: "geosite.dat",
			URLs: []string{
				"https://github.com/Loyalsoldier/v2ray-rules-dat/releases/latest/download/geosite.dat",
				"https://cdn.jsdelivr.net/gh/DustinWin/ruleset_geodata@mihomo-geodata/geosite-all.dat",
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

// GeoDir returns the directory that daed actually loads GEO assets from.
// Prefer DAE_LOCATION_ASSET (set in docker/glider Dockerfile); otherwise the
// project default /usr/local/share/daed — do NOT fall back to /usr/share/dae.
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

// UpdateGeofile downloads GEO files into a /tmp workdir, verifies them, then
// installs into GeoDir() and reloads daed when it is running.
func UpdateGeofile(fileName string) error {
	allow := defaultAllowlist()
	if fileName != "" {
		if _, ok := allow[fileName]; !ok {
			return fmt.Errorf("invalid geofile name: %q (allowed: geoip.dat, geosite.dat, custom.dat)", fileName)
		}
	}

	destDir := GeoDir()
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return fmt.Errorf("create geo dir %s: %w", destDir, err)
	}

	tmpRoot, err := os.MkdirTemp(os.TempDir(), "daed-geoupdate-*")
	if err != nil {
		return fmt.Errorf("create temp dir: %w", err)
	}
	defer func() {
		if rmErr := os.RemoveAll(tmpRoot); rmErr != nil {
			logrus.Warnf("cleanup temp geo dir %s: %v", tmpRoot, rmErr)
		}
	}()

	keys := []string{"geosite.dat", "geoip.dat", "custom.dat"}
	if fileName != "" {
		keys = []string{fileName}
	}

	logrus.Infof("Geofile update: download → %s, install → %s", tmpRoot, destDir)

	type staged struct {
		name    string
		tmpPath string
		dest    string
	}
	stagedFiles := make([]staged, 0, len(keys))

	for _, key := range keys {
		entry := allow[key]
		tmpPath := filepath.Join(tmpRoot, entry.FileName)
		if err := downloadToFile(entry.URLs, tmpPath); err != nil {
			return fmt.Errorf("download %s: %w", entry.FileName, err)
		}
		fi, err := os.Stat(tmpPath)
		if err != nil {
			return fmt.Errorf("stat downloaded %s: %w", entry.FileName, err)
		}
		if fi.Size() <= 0 {
			return fmt.Errorf("downloaded %s is empty", entry.FileName)
		}
		stagedFiles = append(stagedFiles, staged{
			name:    entry.FileName,
			tmpPath: tmpPath,
			dest:    filepath.Join(destDir, entry.FileName),
		})
		logrus.Infof("Geofile downloaded OK: %s (%d bytes)", entry.FileName, fi.Size())
	}

	// Only after every download succeeds, install into the live asset directory.
	for _, f := range stagedFiles {
		if err := installFile(f.tmpPath, f.dest); err != nil {
			return fmt.Errorf("install %s → %s: %w", f.name, f.dest, err)
		}
		logrus.Infof("Geofile installed: %s", f.dest)
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
		if copyErr != nil {
			os.Remove(tmpPath)
			lastErr = fmt.Errorf("%s: copy: %w", url, copyErr)
			continue
		}
		if closeErr != nil {
			os.Remove(tmpPath)
			lastErr = fmt.Errorf("%s: close: %w", url, closeErr)
			continue
		}
		if n <= 0 {
			os.Remove(tmpPath)
			lastErr = fmt.Errorf("%s: empty body", url)
			continue
		}
		return nil
	}
	if lastErr != nil {
		return lastErr
	}
	return fmt.Errorf("no URL succeeded for %s", filepath.Base(tmpPath))
}

// installFile copies a completed temp file into destDir (same-FS rename preferred,
// cross-device falls back to copy then remove the temp source is handled by caller cleanup).
func installFile(tmpPath, destPath string) error {
	if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
		return err
	}

	// Stage beside the destination so rename is atomic on the same filesystem.
	staging, err := os.CreateTemp(filepath.Dir(destPath), filepath.Base(destPath)+".install.*")
	if err != nil {
		return err
	}
	stagingPath := staging.Name()

	src, err := os.Open(tmpPath)
	if err != nil {
		staging.Close()
		os.Remove(stagingPath)
		return err
	}
	_, copyErr := io.Copy(staging, src)
	src.Close()
	closeErr := staging.Close()
	if copyErr != nil || closeErr != nil {
		os.Remove(stagingPath)
		if copyErr != nil {
			return copyErr
		}
		return closeErr
	}

	if err := os.Rename(stagingPath, destPath); err != nil {
		// Cross-device rename can fail; fall back to copy into dest then remove staging.
		if copyErr := copyFile(stagingPath, destPath); copyErr != nil {
			os.Remove(stagingPath)
			return err
		}
		os.Remove(stagingPath)
	}
	_ = os.Chmod(destPath, 0644)

	fi, err := os.Stat(destPath)
	if err != nil {
		return err
	}
	if fi.Size() <= 0 {
		return fmt.Errorf("installed file is empty: %s", destPath)
	}
	return nil
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
	logrus.Infof("Geofile scheduled update enabled: %s (assets dir=%s)", expr, GeoDir())
	return nil
}
