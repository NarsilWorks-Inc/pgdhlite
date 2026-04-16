package pgdhlite

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	dhl "github.com/NarsilWorks-Inc/datahelperlite/v3"
	cfg "github.com/eaglebush/config"
	dn "github.com/eaglebush/datainfo"
	"github.com/segmentio/ksuid"
)

const create_string string = `

DROP TABLE IF EXISTS public.master_table;

CREATE TABLE IF NOT EXISTS public.master_table
(
    pk character varying(30) COLLATE pg_catalog."default" NOT NULL,
    code character varying(15) COLLATE pg_catalog."default" NOT NULL,
    name character varying(50) COLLATE pg_catalog."default" NOT NULL,
    count integer NOT NULL,
    secure boolean,
    time_of_day timestamp without time zone,
    money numeric(18,4),
	byte smallint,
    CONSTRAINT master_table_pkey PRIMARY KEY (pk),
	CONSTRAINT uix_code UNIQUE (code)
);

INSERT INTO public.master_table (pk, code, name, count, secure, time_of_day, money, byte) VALUES ('34VcMI5kdBG9uplzh8rb0NjBWIG', 'CODE1', 'Code Name 1', 100, true, '2025-10-24 18:32:01', 120.54, 1);
INSERT INTO public.master_table (pk, code, name, count, secure, time_of_day, money, byte) VALUES ('34VcMHaZqclpxHUUWvsJIeQ1Rwu', 'CODE2', 'Code Name 2', 200, false, '2025-10-24 18:32:02', 1345.67, 2);
INSERT INTO public.master_table (pk, code, name, count, secure, time_of_day, money, byte) VALUES ('34VcMFUXpxMffOkI2QdRliba9NM', 'CODE3', 'Code Name 3', 300, true, '2025-10-24 18:32:03', 2435.76, 3);
INSERT INTO public.master_table (pk, code, name, count, secure, time_of_day, money, byte) VALUES ('34VcMBUBmlOxQeOkGTx89KXlo5R', 'CODE4', 'Code Name 4', 400, false, '2025-10-24 18:32:04', 6575.543, 4);
INSERT INTO public.master_table (pk, code, name, count, secure, time_of_day, money, byte) VALUES ('34VcMGiPFq2s6fmS9T78RP0klyL', 'CODE5', 'Code Name 5', 500, true, '2025-10-24 18:32:05', 12346.78, 10);
INSERT INTO public.master_table (pk, code, name, count, secure, time_of_day, money, byte) VALUES ('34VcMFvuRRhpWw9p77w0Hv08ht6', 'CODE6', 'Code Name 6', 600, false, '2025-10-24 18:32:06', 6543.77, 20);
INSERT INTO public.master_table (pk, code, name, count, secure, time_of_day, money, byte) VALUES ('34VcMI6pZLLwaEsfJzXUBntkRF4', 'CODE7', 'Code Name 7', 700, true, '2025-10-24 18:32:07', 12343.324, 30);
INSERT INTO public.master_table (pk, code, name, count, secure, time_of_day, money, byte) VALUES ('34VcMFTKfE4kuJFn4ivHNm8XV0R', 'CODE8', 'Code Name 8', 800, false, '2025-10-24 18:32:08', 5644.454, 40);
INSERT INTO public.master_table (pk, code, name, count, secure, time_of_day, money, byte) VALUES ('34VcMFAIOSlViXiknwqJ4tcow1U', 'CODE9', 'Code Name 9', 900, true, '2025-10-24 18:32:09', 3454.655, 100);
INSERT INTO public.master_table (pk, code, name, count, secure, time_of_day, money, byte) VALUES ('34VcMITgqgRJZ79DUGj5w3bwRLZ', 'CODE10', 'Code Name 10', 1000, false, '2025-10-24 18:32:10', 7665.345, 200);
INSERT INTO public.master_table (pk, code, name, count, secure, time_of_day, money, byte) VALUES ('34Vcb4lqxPRXdgOldDvTF6GqsuP', 'CODE11', 'Code Name 11', 1100, false, '2025-10-24 18:32:11', 100.00, 300);
INSERT INTO public.master_table (pk, code, name, count, secure, time_of_day, money, byte) VALUES ('34VccAm9HHIdhost7h5QJXFXdfi', 'CODE12', 'Code Name 12', 1200, true, '2025-10-24 18:32:12', 120.23, 400);
`

func TestGetRows(t *testing.T) {

	// Load configuration
	cf, err := cfg.Load(`config.json`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	// Initialize data info
	cdi := cf.GetDatabaseInfo(`DEFAULT`)
	di := dn.New(
		dn.ConnectionString(cdi.ConnectionString),
		dn.ParameterPlaceHolder(cdi.ParameterPlaceholder),
	)

	// Initialize datahelper handler
	hndl, err := dhl.NewHandle(`pgdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	if err = hndl.Open(di); err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	defer hndl.Close()

	// Create new datahelper lite
	c, err := dhl.New(nil, `pgdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	// Acquire handle and context
	err = c.Acquire(context.Background(), hndl)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	_, err = c.Exec(create_string)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	rows, err := c.Query(`SELECT pk, code, name, count, secure, time_of_day, money, byte FROM public.master_table WHERE count=-1;`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	defer rows.Close()

	var (
		pk, code, name string
		count          int
		secure         bool
		time_of_day    *time.Time
		money          float64
		bytev          uint8
	)

	cols, err := rows.Columns()
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	for _, col := range cols {
		t.Log(col.Name(), col.DatabaseTypeName(), col.ScanType())
	}

	for rows.Next() {
		err = rows.Scan(
			&pk,
			&code,
			&name,
			&count,
			&secure,
			&time_of_day,
			&money,
			&bytev,
		)

		if err != nil {
			t.Log(err.Error())
			t.Fail()
			return
		}

		t.Logf("pk: %s, code: %s, name: %s, count: %d, secure: %t, time_of_day: %s, money: %f, bytev %d",
			pk, code, name, count, secure, time_of_day, money, bytev)
	}

	if err = rows.Err(); err != nil {
		t.Log(err.Error())
		return
	}
}

func TestGetRow(t *testing.T) {
	// Load configuration
	cf, err := cfg.Load(`config.json`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	// Initialize data info
	cdi := cf.GetDatabaseInfo(`DEFAULT`)
	di := dn.New(
		dn.ConnectionString(cdi.ConnectionString),
		dn.ParameterPlaceHolder(cdi.ParameterPlaceholder),
	)

	// Initialize datahelper handler
	hndl, err := dhl.NewHandle(`pgdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	if err = hndl.Open(di); err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	defer hndl.Close()

	// Create new datahelper lite
	c, err := dhl.New(nil, `pgdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	// Acquire handle and context
	err = c.Acquire(context.Background(), hndl)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	_, err = c.Exec(create_string)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	var (
		pk, code, name string
		count          int
		secure         bool
		time_of_day    *time.Time
		money          float64
		bytev          uint8
	)

	err = c.QueryRow(`
		SELECT pk, code, name, count, secure, time_of_day, money, byte
		FROM public.master_table WHERE count=-1;`).Scan(&pk,
		&code,
		&name,
		&count,
		&secure,
		&time_of_day,
		&money,
		&bytev,
	)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	t.Logf("pk: %s, code: %s, name: %s, count: %d, secure: %t, time_of_day: %s, money: %f, bytev: %d",
		pk, code, name, count, secure, time_of_day, money, bytev)

}

func TestExists(t *testing.T) {
	// Load configuration
	cf, err := cfg.Load(`config.json`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	// Initialize data info
	cdi := cf.GetDatabaseInfo(`DEFAULT`)
	di := dn.New(
		dn.ConnectionString(cdi.ConnectionString),
		dn.ParameterPlaceHolder(cdi.ParameterPlaceholder),
	)

	// Initialize datahelper handler
	hndl, err := dhl.NewHandle(`pgdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	if err = hndl.Open(di); err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	defer hndl.Close()

	// Create new datahelper lite
	c, err := dhl.New(nil, `pgdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	// Acquire handle and context
	err = c.Acquire(context.Background(), hndl)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	_, err = c.Exec(create_string)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	exists, err := c.Exists(`public.master_table WHERE count= $1`, 900)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	t.Logf("Exists: %t", exists)

}

func TestQueryArray(t *testing.T) {
	// Load configuration
	cf, err := cfg.Load(`config.json`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	// Initialize data info
	cdi := cf.GetDatabaseInfo(`DEFAULT`)
	di := dn.New(
		dn.ConnectionString(cdi.ConnectionString),
		dn.ParameterPlaceHolder(cdi.ParameterPlaceholder),
	)

	// Initialize datahelper handler
	hndl, err := dhl.NewHandle(`pgdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	if err = hndl.Open(di); err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	defer hndl.Close()

	// Create new datahelper lite
	c, err := dhl.New(nil, `pgdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	// Acquire handle and context
	err = c.Acquire(context.Background(), hndl)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	_, err = c.Exec(create_string)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	//var arr []int
	var arr []int

	err = c.QueryArray(`SELECT count FROM public.master_table;`, &arr)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	t.Logf("Array: %v", arr)

}

func TestWriteTransactions(t *testing.T) {
	// Load configuration
	cf, err := cfg.Load(`config.json`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	// Initialize data info
	cdi := cf.GetDatabaseInfo(`DEFAULT`)
	di := dn.New(
		dn.ConnectionString(cdi.ConnectionString),
		dn.ParameterPlaceHolder(cdi.ParameterPlaceholder),
	)

	// Initialize datahelper handler
	hndl, err := dhl.NewHandle(`pgdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	if err = hndl.Open(di); err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	defer hndl.Close()

	// Create new datahelper lite
	c, err := dhl.New(nil, `pgdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	// Acquire handle and context
	err = c.Acquire(context.Background(), hndl)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	_, err = c.Exec(create_string)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	i := 13
	secure := false
	c.Begin()

	for {
		if i > 999 {
			break
		}
		secure = !secure

		_, err = c.Exec(`
			INSERT INTO public.master_table (
				pk, code, name, count,
				secure, time_of_day, money, byte)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8);`,
			ksuid.New().String(),
			"CODE"+fmt.Sprintf("%d", i),
			"Code Name "+fmt.Sprintf("%d", i),
			i,
			secure,
			time.Now().UTC(),
			i,
			i,
		)

		if err != nil {
			c.Rollback()
			t.Log(err.Error())
			break
		}

		i++
	}

	//c.Rollback()
	c.Commit()

}

func TestWriteNestedWithTransactions(t *testing.T) {

	// Load configuration
	cf, err := cfg.Load(`config.json`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	// Initialize data info
	cdi := cf.GetDatabaseInfo(`DEFAULT`)
	di := dn.New(
		dn.ConnectionString(cdi.ConnectionString),
		dn.ParameterPlaceHolder(cdi.ParameterPlaceholder),
	)

	// Initialize datahelper handler
	hndl, err := dhl.NewHandle(`pgdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	if err = hndl.Open(di); err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	defer hndl.Close()

	// Create new datahelper lite
	c, err := dhl.New(nil, `pgdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	// Acquire handle and context
	err = c.Acquire(context.Background(), hndl)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	_, err = c.Exec(create_string)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	i := 0
	secure := false
	c.Begin()

	for {

		if i > 999 {
			break
		}

		secure = !secure

		// go to a function to reuse
		func(dh dhl.DataHelperLite) {
			c.Begin()

			_, err = c.Exec(`
			INSERT INTO public.master_table (
				pk, code, name, count,
				secure, time_of_day, money, byte)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8);`,
				ksuid.New().String(),
				"CODE"+fmt.Sprintf("%d", i),
				"Code Name "+fmt.Sprintf("%d", i),
				i,
				secure,
				time.Now().UTC(),
				i,
				i,
			)
			if err != nil {
				c.Rollback()
				t.Log(err.Error())
			}

			c.Commit()
		}(c)

		i++
	}

	c.Commit()

}

func TestWriteNested(t *testing.T) {

	// Load configuration
	cf, err := cfg.Load(`config.json`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	// Initialize data info
	cdi := cf.GetDatabaseInfo(`DEFAULT`)
	di := dn.New(
		dn.ConnectionString(cdi.ConnectionString),
		dn.ParameterPlaceHolder(cdi.ParameterPlaceholder),
	)

	// Initialize datahelper handler
	hndl, err := dhl.NewHandle(`pgdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	if err = hndl.Open(di); err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	defer hndl.Close()

	// Create new datahelper lite
	c, err := dhl.New(nil, `pgdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	// Acquire handle and context
	err = c.Acquire(context.Background(), hndl)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	_, err = c.Exec(create_string)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	i := 0
	secure := false
	for {

		if i > 999 {
			break
		}

		secure = !secure

		// go to a function to reuse
		func(dh dhl.DataHelperLite) {
			_, err = c.Exec(`
			INSERT INTO public.master_table (
				pk, code, name, count,
				secure, time_of_day, money, byte)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8);`,
				ksuid.New().String(),
				"CODE"+fmt.Sprintf("%d", i),
				"Code Name "+fmt.Sprintf("%d", i),
				i,
				secure,
				time.Now().UTC(),
				i,
				i,
			)
			if err != nil {
				t.Log(err.Error())
				t.Fail()
			}
		}(c)

		i++
	}

	//c.Close()
}

func TestSequence(t *testing.T) {
	// Load configuration
	cf, err := cfg.Load(`config.json`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	// Initialize data info
	cdi := cf.GetDatabaseInfo(`DEFAULT`)
	di := dn.New(
		dn.ConnectionString(cdi.ConnectionString),
		dn.ParameterPlaceHolder(cdi.ParameterPlaceholder),
	)

	// Initialize datahelper handler
	hndl, err := dhl.NewHandle(`pgdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	if err = hndl.Open(di); err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	defer hndl.Close()

	// Create new datahelper lite
	c, err := dhl.New(nil, `pgdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	// Acquire handle and context
	err = c.Acquire(context.Background(), hndl)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	var seq int64

	err = c.Next(`testsequence`, &seq)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	t.Logf("Sequence for testsequence: %d", seq)
}

func TestUint8AndUInt16(t *testing.T) {
	// Load configuration
	cf, err := cfg.Load(`config.json`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	// Initialize data info
	cdi := cf.GetDatabaseInfo(`DEFAULT`)
	di := dn.New(
		dn.ConnectionString(cdi.ConnectionString),
		dn.ParameterPlaceHolder(cdi.ParameterPlaceholder),
	)

	// Initialize datahelper handler
	hndl, err := dhl.NewHandle(`pgdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	if err = hndl.Open(di); err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	defer hndl.Close()

	// Create new datahelper lite
	c, err := dhl.New(nil, `pgdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	// Acquire handle and context
	err = c.Acquire(context.Background(), hndl)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	_, err = c.Exec(create_string)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	var (
		bytev *uint8
		count *uint16
	)

	err = c.QueryRow(`SELECT byte, count FROM public.master_table WHERE pk='34Vcb4lqxPRXdgOldDvTF6GqsuP';`).Scan(&bytev, &count)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	t.Logf("Byte: %d, Count %d", *bytev, *count)

}

func TestFloat32(t *testing.T) {
	// Load configuration
	cf, err := cfg.Load(`config.json`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	// Initialize data info
	cdi := cf.GetDatabaseInfo(`DEFAULT`)
	di := dn.New(
		dn.ConnectionString(cdi.ConnectionString),
		dn.ParameterPlaceHolder(cdi.ParameterPlaceholder),
	)

	// Initialize datahelper handler
	hndl, err := dhl.NewHandle(`pgdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	if err = hndl.Open(di); err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	defer hndl.Close()

	// Create new datahelper lite
	c, err := dhl.New(nil, `pgdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	// Acquire handle and context
	err = c.Acquire(context.Background(), hndl)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	_, err = c.Exec(create_string)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	var (
		money *float32
	)

	err = c.QueryRow(`SELECT money FROM public.master_table WHERE pk='34VcMI6pZLLwaEsfJzXUBntkRF4';`).Scan(&money)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	t.Logf("QtyOrigin: %f", *money)

}

func TestJsonRawMessage(t *testing.T) {
	// Load configuration
	cf, err := cfg.Load(`config.json`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	// Initialize data info
	cdi := cf.GetDatabaseInfo(`HAWKEYE`)
	di := dn.New(
		dn.ConnectionString(cdi.ConnectionString),
		dn.ParameterPlaceHolder(cdi.ParameterPlaceholder),
	)

	// Initialize datahelper handler
	hndl, err := dhl.NewHandle(`pgdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	if err = hndl.Open(di); err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	defer hndl.Close()

	// Create new datahelper lite
	c, err := dhl.New(nil, `pgdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	// Acquire handle and context
	err = c.Acquire(context.Background(), hndl)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	// _, err = c.Exec(create_string)
	// if err != nil {
	// 	t.Log(err.Error())
	// 	t.Fail()
	// 	return
	// }

	var (
		metadataPtr *json.RawMessage
		metadata    json.RawMessage
		//address     json.RawMessage
	)

	// err = c.QueryRow(`SELECT metadata FROM org_nodes WHERE id=28;`).Scan(&metadataPtr)
	// if err != nil {
	// 	t.Log(err.Error())
	// 	t.Fail()
	// 	return
	// }

	// err = c.QueryRow(`SELECT metadata FROM org_nodes WHERE id=28;`).Scan(&metadata)
	// if err != nil {
	// 	t.Log(err.Error())
	// 	t.Fail()
	// 	return
	// }

	rows, err := c.Query(`SELECT metadata FROM org_nodes WHERE id=28;`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	defer rows.Close()

	for rows.Next() {
		if err = rows.Scan(&metadataPtr); err != nil {
			t.Log(err.Error())
			t.Fail()
			return
		}
	}

	if err = rows.Err(); err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	// err = c.QueryRow(`SELECT address FROM locations WHERE node_id=27;`).Scan(&address)
	// if err != nil {
	// 	t.Log(err.Error())
	// 	t.Fail()
	// 	return
	// }

	t.Logf("Data: %v, %v", *metadataPtr, metadata)

}

func TestUpsertReturning(t *testing.T) {
	// Load configuration
	cf, err := cfg.Load(`config.json`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	// Initialize data info
	cdi := cf.GetDatabaseInfo(`HAWKEYE`)
	di := dn.New(
		dn.ConnectionString(cdi.ConnectionString),
		dn.ParameterPlaceHolder(cdi.ParameterPlaceholder),
	)

	// Initialize datahelper handler
	hndl, err := dhl.NewHandle(`pgdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	if err = hndl.Open(di); err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	defer hndl.Close()

	// Create new datahelper lite
	c, err := dhl.New(nil, `pgdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	// Acquire handle and context
	err = c.Acquire(context.Background(), hndl)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	_, err = c.Exec(create_string)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	var (
		pk string
	)

	row, err := c.UpsertReturning(
		"master_table",
		[]string{"pk", "code", "name", "count"},
		[]string{"code"},
		[]string{"name"},
		[]string{"pk"},
		"34VcMHaZqclpxHUUWvsJIeQ1Rwu", "CODE2", "Code Name 2000", 200,
	)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	if err = row.Scan(&pk); err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	t.Logf("PK: %s", pk)
}
