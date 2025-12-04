// Copyright 2025 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Notes: it's a copy from tidb

package types

import (
	"fmt"
	gotime "time"

	"github.com/pingcap/errors"
)

// CoreTime is the internal struct type for Time.
type CoreTime uint64

// ZeroCoreTime is the zero value for TimeInternal type.
var ZeroCoreTime = CoreTime(0)

// String implements fmt.Stringer.
func (t CoreTime) String() string {
	return fmt.Sprintf("{%d %d %d %d %d %d %d}", t.getYear(), t.getMonth(), t.getDay(), t.getHour(), t.getMinute(), t.getSecond(), t.getMicrosecond())
}

func (t CoreTime) getYear() uint16 {
	return uint16((uint64(t) & yearBitFieldMask) >> yearBitFieldOffset)
}

// Year returns the year value.
func (t CoreTime) Year() int {
	return int(t.getYear())
}

func (t CoreTime) getMonth() uint8 {
	return uint8((uint64(t) & monthBitFieldMask) >> monthBitFieldOffset)
}

// Month returns the month value.
func (t CoreTime) Month() int {
	return int(t.getMonth())
}

func (t CoreTime) getDay() uint8 {
	return uint8((uint64(t) & dayBitFieldMask) >> dayBitFieldOffset)
}

// Day returns the day value.
func (t CoreTime) Day() int {
	return int(t.getDay())
}

func (t CoreTime) getHour() uint8 {
	return uint8((uint64(t) & hourBitFieldMask) >> hourBitFieldOffset)
}

// Hour returns the hour value.
func (t CoreTime) Hour() int {
	return int(t.getHour())
}

func (t CoreTime) getMinute() uint8 {
	return uint8((uint64(t) & minuteBitFieldMask) >> minuteBitFieldOffset)
}

// Minute returns the minute value.
func (t CoreTime) Minute() int {
	return int(t.getMinute())
}

func (t CoreTime) getSecond() uint8 {
	return uint8((uint64(t) & secondBitFieldMask) >> secondBitFieldOffset)
}

// Second returns the second value.
func (t CoreTime) Second() int {
	return int(t.getSecond())
}

func (t CoreTime) getMicrosecond() uint32 {
	return uint32((uint64(t) & microsecondBitFieldMask) >> microsecondBitFieldOffset)
}

// Microsecond returns the microsecond value.
func (t CoreTime) Microsecond() int {
	return int(t.getMicrosecond())
}

// Weekday returns weekday value.
func (t CoreTime) Weekday() gotime.Weekday {
	// No need to consider timezone, use the date directly.
	t1, err := t.GoTime(gotime.UTC)
	// allow invalid dates
	if err != nil {
		return t1.Weekday()
	}
	return t1.Weekday()
}

// YearWeek returns year and week.
func (t CoreTime) YearWeek(mode int) (year int, week int) {
	behavior := weekMode(mode) | weekBehaviourYear
	return calcWeek(t, behavior)
}

// Week returns week value.
func (t CoreTime) Week(mode int) int {
	if t.getMonth() == 0 || t.getDay() == 0 {
		return 0
	}
	_, week := calcWeek(t, weekMode(mode))
	return week
}

// YearDay returns year and day.
func (t CoreTime) YearDay() int {
	if t.getMonth() == 0 || t.getDay() == 0 {
		return 0
	}
	year, month, day := t.Year(), t.Month(), t.Day()
	return calcDaynr(year, month, day) -
		calcDaynr(year, 1, 1) + 1
}

// GoTime converts Time to GoTime.
func (t CoreTime) GoTime(loc *gotime.Location) (gotime.Time, error) {
	// gotime.Time can't represent month 0 or day 0, date contains 0 would be converted to a nearest date,
	// For example, 2006-12-00 00:00:00 would become 2015-11-30 23:59:59.
	year, month, day, hour, minute, second, microsecond := t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Microsecond()
	tm := gotime.Date(year, gotime.Month(month), day, hour, minute, second, microsecond*1000, loc)
	year2, month2, day2 := tm.Date()
	hour2, minute2, second2 := tm.Clock()
	microsec2 := tm.Nanosecond() / 1000
	// This function will check the result, and return an error if it's not the same with the origin input .
	if year2 != year || int(month2) != month || day2 != day ||
		hour2 != hour || minute2 != minute || second2 != second ||
		microsec2 != microsecond {
		return tm, errors.Trace(ErrWrongValue.GenWithStackByArgs(TimeStr, t))
	}
	return tm, nil
}

// AdjustedGoTime converts Time to GoTime and adjust for invalid DST times
// like during the DST change with increased offset,
// normally moving to Daylight Saving Time.
// see https://github.com/pingcap/tidb/issues/28739
func (t CoreTime) AdjustedGoTime(loc *gotime.Location) (gotime.Time, error) {
	tm, err := t.GoTime(loc)
	if err == nil {
		return tm, nil
	}

	// The converted go time did not map back to the same time, probably it was between a
	// daylight saving transition, adjust the time to the closest Zone bound.
	start, end := tm.ZoneBounds()
	// time zone transitions are normally 1 hour, allow up to 4 hours before returning error
	if start.Sub(tm).Abs().Hours() > 4.0 && end.Sub(tm).Abs().Hours() > 4.0 {
		return tm, errors.Trace(ErrWrongValue.GenWithStackByArgs(TimeStr, tm))
	}
	// use the closest transition time
	if tm.Sub(start).Abs() <= tm.Sub(end).Abs() {
		return start, nil
	}
	return end, nil
}

// IsLeapYear returns if it's leap year.
func (t CoreTime) IsLeapYear() bool {
	return isLeapYear(t.getYear())
}

func isLeapYear(year uint16) bool {
	return (year%4 == 0 && year%100 != 0) || year%400 == 0
}

var daysByMonth = [12]int{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}

// GetLastDay returns the last day of the month
func GetLastDay(year, month int) int {
	var day = 0
	if month > 0 && month <= 12 {
		day = daysByMonth[month-1]
	}
	if month == 2 && isLeapYear(uint16(year)) {
		day = 29
	}
	return day
}

func getFixDays(year, month, day int, ot gotime.Time) int {
	if (year != 0 || month != 0) && day == 0 {
		od := ot.Day()
		t := ot.AddDate(year, month, day)
		td := t.Day()
		if od != td {
			tm := int(t.Month()) - 1
			tMax := GetLastDay(t.Year(), tm)
			dd := tMax - od
			return dd
		}
	}
	return 0
}

// AddDate fix gap between mysql and golang api
// When we execute select date_add('2018-01-31',interval 1 month) in mysql we got 2018-02-28
// but in tidb we got 2018-03-03.
// Dig it and we found it's caused by golang api time.Date(year int, month Month, day, hour, min, sec, nsec int, loc *Location) Time ,
// it says October 32 converts to November 1 ,it conflicts with mysql.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_date-add
func AddDate(year, month, day int64, ot gotime.Time) (nt gotime.Time, _ error) {
	// We must limit the range of year, month and day to avoid overflow.
	// The datetime range is from '1000-01-01 00:00:00.000000' to '9999-12-31 23:59:59.499999',
	// so it is safe to limit the added value from -10000*365 to 10000*365.
	const maxAdd = 10000 * 365
	const minAdd = -maxAdd
	if year > maxAdd || year < minAdd ||
		month > maxAdd || month < minAdd ||
		day > maxAdd || day < minAdd {
		return nt, ErrDatetimeFunctionOverflow.GenWithStackByArgs("datetime")
	}

	df := getFixDays(int(year), int(month), int(day), ot)
	if df != 0 {
		nt = ot.AddDate(int(year), int(month), df)
	} else {
		nt = ot.AddDate(int(year), int(month), int(day))
	}

	if nt.Year() < 0 || nt.Year() > 9999 {
		return nt, ErrDatetimeFunctionOverflow.GenWithStackByArgs("datetime")
	}

	return nt, nil
}

// calcDaynr calculates days since 0000-00-00.
func calcDaynr(year, month, day int) int {
	if year == 0 && month == 0 {
		return 0
	}

	delsum := 365*year + 31*(month-1) + day
	if month <= 2 {
		year--
	} else {
		delsum -= (month*4 + 23) / 10
	}
	temp := ((year/100 + 1) * 3) / 4
	return delsum + year/4 - temp
}

// DateDiff calculates number of days between two days.
func DateDiff(startTime, endTime CoreTime) int {
	return calcDaynr(startTime.Year(), startTime.Month(), startTime.Day()) - calcDaynr(endTime.Year(), endTime.Month(), endTime.Day())
}

// calcDaysInYear calculates days in one year, it works with 0 <= year <= 99.
func calcDaysInYear(year int) int {
	if (year&3) == 0 && (year%100 != 0 || (year%400 == 0 && (year != 0))) {
		return 366
	}
	return 365
}

// calcWeekday calculates weekday from daynr, returns 0 for Monday, 1 for Tuesday ...
func calcWeekday(daynr int, sundayFirstDayOfWeek bool) int {
	daynr += 5
	if sundayFirstDayOfWeek {
		daynr++
	}
	return daynr % 7
}

type weekBehaviour uint

const (
	// weekBehaviourMondayFirst set Monday as first day of week; otherwise Sunday is first day of week
	weekBehaviourMondayFirst weekBehaviour = 1 << iota
	// If set, Week is in range 1-53, otherwise Week is in range 0-53.
	// Note that this flag is only relevant if WEEK_JANUARY is not set.
	weekBehaviourYear
	// If not set, Weeks are numbered according to ISO 8601:1988.
	// If set, the week that contains the first 'first-day-of-week' is week 1.
	weekBehaviourFirstWeekday
)

func (v weekBehaviour) test(flag weekBehaviour) bool {
	return (v & flag) != 0
}

func weekMode(mode int) weekBehaviour {
	weekFormat := weekBehaviour(mode & 7)
	if (weekFormat & weekBehaviourMondayFirst) == 0 {
		weekFormat ^= weekBehaviourFirstWeekday
	}
	return weekFormat
}

// calcWeek calculates week and year for the time.
func calcWeek(t CoreTime, wb weekBehaviour) (year int, week int) {
	var days int
	ty, tm, td := int(t.getYear()), int(t.getMonth()), int(t.getDay())
	daynr := calcDaynr(ty, tm, td)
	firstDaynr := calcDaynr(ty, 1, 1)
	mondayFirst := wb.test(weekBehaviourMondayFirst)
	weekYear := wb.test(weekBehaviourYear)
	firstWeekday := wb.test(weekBehaviourFirstWeekday)

	weekday := calcWeekday(firstDaynr, !mondayFirst)

	year = ty

	if tm == 1 && td <= 7-weekday {
		if !weekYear &&
			((firstWeekday && weekday != 0) || (!firstWeekday && weekday >= 4)) {
			week = 0
			return
		}
		weekYear = true
		year--
		days = calcDaysInYear(year)
		firstDaynr -= days
		weekday = (weekday + 53*7 - days) % 7
	}

	if (firstWeekday && weekday != 0) ||
		(!firstWeekday && weekday >= 4) {
		days = daynr - (firstDaynr + 7 - weekday)
	} else {
		days = daynr - (firstDaynr - weekday)
	}

	if weekYear && days >= 52*7 {
		weekday = (weekday + calcDaysInYear(year)) % 7
		if (!firstWeekday && weekday < 4) ||
			(firstWeekday && weekday == 0) {
			year++
			week = 1
			return
		}
	}
	week = days/7 + 1
	return
}
