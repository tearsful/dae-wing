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

const DefaultCron = "0 0 10 * * 4" // Thursday 10:00 (with seconds)

var cronParser = cron.NewParser(
	cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
)

type geofileEntry struct {
	FileName string
	URLs     []string
}

func defaultAllowlist() map[string]geofileEntry {
	return map[string]geofileEntry{
		"geoip.dat": {
			FileName: "geoip.dat",
			URLs: []string{
				"https://cdn.jsdelivr.net/gh/DustinWin/ruleset_geodata@mihomo-geodata/geoip-all.dat",
				"https://github.com/Loyalsoldier/v2ray-rules-dat/releases/latest/download/geoip.dat",
			},
		},
		"geosite.dat": {
			FileName: "geosite.dat",
			URLs: []string{
				"https://cdn.jsdelivr.net/gh/DustinWin/ruleset_geodata@mihomo-geodata/geosite-all.dat",
				"https://github.com/Loyalsoldier/v2ray-rules-dat/releases/latest/download/geosite.dat",
			},
		},
	}
}

func GeoDir() string {
	if v := strings.TrimSpace(os.Getenv("DAE_LOCATION_ASSET")); v != "" {
		return v
	}
	for _, p := range []string{"/usr/local/share/daed", "/usr/local/share/dae", "/usr/share/dae"} {
		if st, err := os.Stat(p); err == nil && st.IsDir() {
			return p
		}
	}
	return "/usr/local/share/daed"
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

func UpdateGeofile(fileName string) error {
	allow := defaultAllowlist()
	if fileName != "" {
		if _, ok := allow[fileName]; !ok {
			return fmt.Errorf("invalid geofile name: %q", fileName)
		}
	}

	dir := GeoDir()
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	targets := make([]geofileEntry, 0, 2)
	if fileName == "" {
		for _, key := range []string{"geosite.dat", "geoip.dat"} {
			targets = append(targets, allow[key])
		}
	} else {
		targets = append(targets, allow[fileName])
	}

	var errs []string
	for _, entry := range targets {
		dest := filepath.Join(dir, entry.FileName)
		if err := downloadFile(entry.URLs, dest); err != nil {
			errs = append(errs, err.Error())
			continue
		}
		logrus.Infof("Geofile updated: %s", dest)
	}
	if len(errs) > 0 {
		return fmt.Errorf("%s", strings.Join(errs, "; "))
	}

	return reloadIfRunning()
}

func downloadFile(urls []string, destPath string) error {
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
			lastErr = fmt.Errorf("download %s: %w", url, err)
			logrus.Warn(lastErr)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			lastErr = fmt.Errorf("download %s: HTTP %d", url, resp.StatusCode)
			logrus.Warn(lastErr)
			continue
		}
		tmp, err := os.CreateTemp(filepath.Dir(destPath), filepath.Base(destPath)+".tmp.*")
		if err != nil {
			resp.Body.Close()
			return err
		}
		tmpPath := tmp.Name()
		_, copyErr := io.Copy(tmp, resp.Body)
		closeErr := tmp.Close()
		resp.Body.Close()
		if copyErr != nil || closeErr != nil {
			os.Remove(tmpPath)
			if copyErr != nil {
				lastErr = copyErr
			} else {
				lastErr = closeErr
			}
			continue
		}
		if err := os.Rename(tmpPath, destPath); err != nil {
			os.Remove(tmpPath)
			lastErr = err
			continue
		}
		_ = os.Chmod(destPath, 0644)
		return nil
	}
	if lastErr != nil {
		return lastErr
	}
	return fmt.Errorf("download failed: no URL for %s", filepath.Base(destPath))
}

func reloadIfRunning() error {
	var sys db.System
	if err := db.DB(context.TODO()).Model(&db.System{}).FirstOrCreate(&sys).Error; err != nil {
		return err
	}
	if !sys.Running {
		return nil
	}
	tx := db.BeginTx(context.TODO())
	if _, err := config.Run(tx, false); err != nil {
		tx.Rollback()
		return fmt.Errorf("geofile updated but reload failed: %w", err)
	}
	tx.Commit()
	logrus.Info("Geofile update applied; dae reloaded")
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
	logrus.Infof("Geofile scheduled update: %s", expr)
	return nil
}
