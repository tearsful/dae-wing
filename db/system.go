/*
 * SPDX-License-Identifier: AGPL-3.0-only
 * Copyright (c) 2023, daeuniverse Organization <team@v2raya.org>
 */

package db

import "gorm.io/gorm"

// DefaultGeoUpdateCron is Thursday 10:00:00 local time (seconds-enabled cron).
const DefaultGeoUpdateCron = "0 0 10 * * 4"

type System struct {
	ID                     uint   `gorm:"primaryKey;autoIncrement"`
	Running                bool   `gorm:"not null;default:false"`
	RunningConfigVersion   uint   `gorm:"not null;default:0"`
	RunningDnsVersion      uint   `gorm:"not null;default:0"`
	RunningRoutingVersion  uint   `gorm:"not null;default:0"`
	RunningGroupVersionSum uint   `gorm:"not null;default:0"`
	RunningGroupIds        string `gorm:"not null;default:''"`
	// GeoUpdateCron is a seconds-enabled cron expression for geosite/geoip refresh.
	// "off" disables the job. Legacy empty is migrated to DefaultGeoUpdateCron on start.
	// Default for new rows: Thursday 10:00.
	GeoUpdateCron string `gorm:"not null;default:'0 0 10 * * 4'"`

	// Foreign keys.
	RunningConfigID  *uint
	RunningConfig    *Config
	RunningDnsID     *uint
	RunningDns       *Dns
	RunningRoutingID *uint
	RunningRouting   *Routing
	RunningGroups    []Group
}

// FirstOrCreateSystem creates the singleton System row if needed.
// Attrs ensures GeoUpdateCron gets DefaultGeoUpdateCron on INSERT — plain
// FirstOrCreate would persist Go's zero value "" and bypass the SQL default,
// accidentally disabling scheduled GEO updates on fresh installs.
func FirstOrCreateSystem(d *gorm.DB, sys *System) *gorm.DB {
	return d.Attrs(System{GeoUpdateCron: DefaultGeoUpdateCron}).FirstOrCreate(sys)
}
