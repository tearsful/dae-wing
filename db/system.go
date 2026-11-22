/*
 * SPDX-License-Identifier: AGPL-3.0-only
 * Copyright (c) 2023, daeuniverse Organization <team@v2raya.org>
 */

package db

type System struct {
	ID                     uint   `gorm:"primaryKey;autoIncrement"`
	Running                bool   `gorm:"not null;default:false"`
	RunningConfigVersion   uint   `gorm:"not null;default:0"`
	RunningDnsVersion      uint   `gorm:"not null;default:0"`
	RunningRoutingVersion  uint   `gorm:"not null;default:0"`
	RunningGroupVersionSum uint   `gorm:"not null;default:0"`
	RunningGroupIds        string `gorm:"not null;default:''"`
	// GeoUpdateCron is a seconds-enabled cron expression for geosite/geoip refresh.
	// Empty string disables the job. Default: Thursday 10:00.
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
