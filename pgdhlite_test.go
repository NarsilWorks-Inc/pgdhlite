package pgdhlite

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	dhl "github.com/NarsilWorks-Inc/datahelperlite"
	//dhl "eaglebush/datahelperlite"

	cfg "github.com/eaglebush/config"
)

/*
	-- Table: public.tnfemailsent

	-- DROP TABLE public.tnfemailsent;

	CREATE TABLE public.tnfemailsent
	(
		email_key integer NOT NULL,
		subject character varying(255) COLLATE pg_catalog."default" NOT NULL,
		body text COLLATE pg_catalog."default" NOT NULL,
		format character varying(10) COLLATE pg_catalog."default" NOT NULL,
		importance integer NOT NULL,
		sender_name character varying(50) COLLATE pg_catalog."default" NOT NULL,
		sender_address character varying(85) COLLATE pg_catalog."default" NOT NULL,
		application_id character varying(50) COLLATE pg_catalog."default" NOT NULL,
		date_queued timestamp without time zone,
		date_sent timestamp without time zone NOT NULL
	)
	WITH (
		OIDS = FALSE
	)
	TABLESPACE zagada;

	ALTER TABLE public.tnfemailsent
		OWNER to postgres;
*/

func TestGetRows(t *testing.T) {

	var (
		err error
		c   dhl.DataHelperLite
	)

	c, err = dhl.New(nil, `pgdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	cf, err := cfg.LoadConfig(`config.json`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	if err = c.Open(context.Background(), cf.GetDatabaseInfo(`DEFAULT`)); err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	defer c.Close()

	rows, err := c.Query(`SELECT email_key, subject, format, sender_name, sender_address, date_queued FROM tnfemailsent;`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	defer rows.Close()

	var (
		emailkey                            int64
		subject, format, sender, senderaddr string
		datequeued                          time.Time
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
			&emailkey,
			&subject,
			&format,
			&sender,
			&senderaddr,
			&datequeued)

		if err != nil {
			t.Log(err.Error())
			t.Fail()
			return
		}

		// t.Logf("email_key: %d, Subject: %s, Format: %s, Sender: %s, SenderAddress: %s, Date Queued: %s",
		// 	emailkey, subject, format, sender, senderaddr, datequeued.Format(`2006-01-02T15:04:05.000Z`))

		t.Logf("email_key: %d, Subject: %s, Format: %s, Sender: %s, SenderAddress: %s, Date Queued: %s",
			emailkey, subject, format, sender, senderaddr, datequeued)
	}

	if rows.Err() != nil {
		t.Log(err.Error())
		return
	}
}

func TestGetRow(t *testing.T) {
	var (
		err error
		c   dhl.DataHelperLite
	)

	//c = &SQLServerHelper{}

	c, err = dhl.New(nil, `pgdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	cf, err := cfg.LoadConfig(`config.json`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	if err = c.Open(context.Background(), cf.GetDatabaseInfo(`DEFAULT`)); err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	defer c.Close()

	/*
		// simple variables

		var (
			emailkey                            int64
			subject, format, sender, senderaddr string
			datequeued                          time.Time
		)

		err = c.QueryRow(`SELECT email_key, subject, format,
									sender_name, sender_address, date_queued
									FROM tnfemailsent
									WHERE email_key = 1;`).Scan(
			&emailkey,
			&subject,
			&format,
			&sender,
			&senderaddr,
			&datequeued)

		t.Logf("EmailKey: %v, Subject: %v, Format: %v, Sender: %v, SenderAddress: %v, Date Queued: %v",
			emailkey, subject, format, sender, senderaddr, datequeued)

	*/

	/*
		// struct with pointer items

		type teststruct struct {
			EmailKey   *int
			Subject    *string
			Format     *string
			Sender     *string
			SenderAddr *string
			DateQueued *time.Time
		}

		ts := teststruct{}

		err = c.QueryRow(`SELECT email_key, subject, format,
									sender_name, sender_address, date_queued
									FROM tnfemailsent
									WHERE email_key = 1;`).Scan(
			&ts.EmailKey,
			&ts.Subject,
			&ts.Format,
			&ts.Sender,
			&ts.SenderAddr,
			&ts.DateQueued)

		t.Logf("EmailKey: %v, Subject: %v, Format: %v, Sender: %v, SenderAddress: %v, Date Queued: %v",
			*ts.EmailKey, *ts.Subject, *ts.Format, *ts.Sender, *ts.SenderAddr, *ts.DateQueued)

	*/

	// struct with normal items

	type teststruct struct {
		EmailKey   int
		Subject    string
		Format     string
		Sender     string
		SenderAddr string
		DateQueued time.Time
	}

	ts := teststruct{}

	err = c.QueryRow(`SELECT email_key, subject, format,
						sender_name, sender_address, date_queued
						FROM tnfemailsent
						WHERE email_key = 224012;`).Scan(
		&ts.EmailKey,
		&ts.Subject,
		&ts.Format,
		&ts.Sender,
		&ts.SenderAddr,
		&ts.DateQueued)

	if err != nil {

		if !errors.Is(err, dhl.ErrNoRows) {
			t.Log(err.Error())
			t.Fail()
			return
		}

		t.Log(err.Error())

	}

	t.Logf("email_key: %v, Subject: %v, Format: %v, Sender: %v, SenderAddress: %v, Date Queued: %v",
		ts.EmailKey, ts.Subject, ts.Format, ts.Sender, ts.SenderAddr, ts.DateQueued)

}

func TestWriteTransactions(t *testing.T) {

	var (
		err error
		//affr int64
		c dhl.DataHelperLite
	)

	//c = &SQLServerHelper{}

	c, err = dhl.New(nil, `pgdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	cf, err := cfg.LoadConfig(`config.json`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	if err = c.Open(context.Background(), cf.GetDatabaseInfo(`DEFAULT`)); err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	defer c.Close()

	i := 0

	c.Begin()

	for {

		if i > 99999 {
			break
		}

		_, err = c.Exec(`INSERT INTO tnfemailsent (
								email_key, subject, body,
								format, importance, sender_name,
								sender_address, application_id,
								date_queued, date_sent
							) VALUES (nextval('testsequence'), 'Subject 1', 'Message' || $1,
									'HTML', 1, 'Me',
									'me@message.com', 'TestApp',
									 $2, timezone('utc'::text, CURRENT_TIMESTAMP));`, fmt.Sprintf("%d", i), time.Now().UTC())
		if err != nil {
			c.Rollback()
			t.Log(err.Error())
			break
		}

		/*

			if (i % 5) == 0 {
				err = c.Mark(`MO`)
				if err != nil {
					t.Logf(err.Error())
				}
			}

			if (i % 10) == 0 {
				//err = c.Save(`MO`)
				err = c.Discard(`MO`)
				if err != nil {
					t.Logf(err.Error())
				}
			}
		*/

		//t.Logf("%d affected rows", affr)

		i++
	}

	//c.Rollback()
	c.Commit()

}

func TestWriteNestedWithTransactions(t *testing.T) {

	var (
		err error
		//affr int64
		c dhl.DataHelperLite
	)

	c, err = dhl.New(nil, `pgdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	cf, err := cfg.LoadConfig(`config.json`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	if err = c.Open(context.Background(), cf.GetDatabaseInfo(`DEFAULT`)); err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	defer c.Close()

	i := 0

	c.Begin()

	for {

		if i > 999 {
			break
		}

		// go to a function to reuse
		func(dh *dhl.DataHelperLite) {

			x, err := dhl.New(dh, `pgdhlite`)
			if err != nil {
				t.Log(err.Error())
				t.Fail()
				return
			}

			if err = x.Open(context.Background(), cf.GetDatabaseInfo(`DEFAULT`)); err != nil {
				t.Log(err.Error())
				t.Fail()
				return
			}
			defer x.Close()

			x.Begin()

			_, err = x.Exec(`INSERT INTO tnfemailsent (
									email_key, subject, body,
									format, importance, sender_name,
									sender_address, application_id,
									date_queued, date_sent
								) VALUES (nextval('testsequence'), 'Subject 1', 'Message' || $1,
										'HTML', 1, 'Me',
										'me@message.com', 'TestApp',
										 $2, timezone('utc'::text, CURRENT_TIMESTAMP));`, fmt.Sprintf("%d", i), time.Now().UTC())
			if err != nil {
				x.Rollback()
				t.Log(err.Error())
			}

			x.Commit()
		}(&c)

		i++
	}

	c.Commit()

}

func TestWriteNested(t *testing.T) {

	var (
		err error
		//affr int64
		c dhl.DataHelperLite
	)

	c, err = dhl.New(nil, `pgdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	cf, err := cfg.LoadConfig(`config.json`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	ctx := context.Background()

	if err = c.Open(ctx, cf.GetDatabaseInfo(`DEFAULT`)); err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	//defer c.Close()

	i := 0

	for {

		if i > 999 {
			break
		}

		// go to a function to reuse
		func(dh *dhl.DataHelperLite) {

			x, err := dhl.New(dh, `pgdhlite`)
			if err != nil {
				t.Log(err.Error())
				t.Fail()
				return
			}

			if err = x.Open(ctx, cf.GetDatabaseInfo(`DEFAULT`)); err != nil {
				t.Log(err.Error())
				t.Fail()
				return
			}
			defer x.Close()

			_, err = x.Exec(`INSERT INTO tnfemailsent (
									email_key, subject, body,
									format, importance, sender_name,
									sender_address, application_id,
									date_queued, date_sent
								) VALUES (nextval('testsequence'), 'Subject 1', 'Message' || $1,
										'HTML', 1, 'Me',
										'me@message.com', 'TestApp',
										 $2, timezone('utc'::text, CURRENT_TIMESTAMP));`, fmt.Sprintf("%d", i), time.Now().UTC())
			if err != nil {
				t.Log(err.Error())
				t.Fail()
			}
		}(&c)

		i++
	}

	//c.Close()
}

func TestSequence(t *testing.T) {
	var (
		err error
		//affr int64
		c dhl.DataHelperLite
	)

	c, err = dhl.New(nil, `pgdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	cf, err := cfg.LoadConfig(`config.json`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	if err = c.Open(context.Background(), cf.GetDatabaseInfo(`DEFAULT`)); err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	defer c.Close()

	var seq int64

	err = c.Next(`testsequence`, &seq)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	t.Logf("Sequence for testsequence: %d", seq)
}

func TestExists(t *testing.T) {
	var (
		err    error
		exists bool
		c      dhl.DataHelperLite
	)

	//c = &SQLServerHelper{}

	c, err = dhl.New(nil, `pgdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	cf, err := cfg.LoadConfig(`config.json`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	if err = c.Open(context.Background(), cf.GetDatabaseInfo(`DEFAULT`)); err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	defer c.Close()

	exists, err = c.Exists(`tnfemailsent WHERE email_key = $1`, 231012)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	t.Logf("Exists: %t", exists)

}

func TestQueryArray(t *testing.T) {
	var (
		err    error
		exists bool
		c      dhl.DataHelperLite
	)

	//c = &SQLServerHelper{}

	c, err = dhl.New(nil, `pgdhlite`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	cf, err := cfg.LoadConfig(`config.json`)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	if err = c.Open(context.Background(), cf.GetDatabaseInfo(`DEFAULT`)); err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}
	defer c.Close()

	//var arr []int
	var arr []int

	//err = c.QueryArray(`SELECT EmailKey FROM tnfEmailSent;`, &arr)
	err = c.QueryArray(`SELECT email_key FROM tnfemailsent;`, &arr)
	if err != nil {
		t.Log(err.Error())
		t.Fail()
		return
	}

	t.Logf("Exists: %t", exists)

}
