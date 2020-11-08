package main

import (
	"encoding/csv"
	"fmt"
	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/jmoiron/sqlx"
	"log"
	"os"
	"strconv"
	"time"
)

type MobileInfno struct {
	Number int64
	OwnerId string
	MNC int64
	Route string
	RegionCode int
	PortDate string
}


//CREATE TABLE IF NOT EXISTS MobileInfo (
//number Int64,
//ownerId String,
//mnc Int64,
//route String,
//regionCode Int,
//portDate String
//) ENGINE = MergeTree()
//ORDER BY Number
//SETTINGS index_granularity = 8192


var (
	USER = ""
	HOST = ""
	PORT = 0
	PASS = ""
	SIZE =  1<<15
	layout = "2006-01-02 15:04:05"
)

func main(){

	db, err := sqlx.Open("clickhouse", "")
	if err != nil {
		log.Fatal(err)
	}

	//var auths []ssh.AuthMethod
	//if aconn, err := net.Dial("unix", os.Getenv("SSH_AUTH_SOCK")); err == nil {
	//	auths = append(auths, ssh.PublicKeysCallback(agent.NewClient(aconn).Signers))
	//
	//}
	//
	//auths = append(auths, ssh.Password(PASS))
	//
	//
	//config := ssh.ClientConfig{
	//	User: USER,
	//	Auth: auths,
	//	HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	//}
	//addr := fmt.Sprintf("%s:%d", HOST, PORT)
	//conn, err := ssh.Dial("tcp", addr, &config)
	//if err != nil {
	//	log.Fatalf("unable to connect to [%s]: %v", addr, err)
	//}
	//defer conn.Close()
	//
	//c, err := sftp.NewClient(conn, sftp.MaxPacket(SIZE))
	//if err != nil {
	//	log.Fatalf("unable to start sftp subsytem: %v", err)
	//}
	//defer c.Close()
	//
	//r, err := c.Open("/numlex/Port_All/Port_All_202010010000_2507.zip")
	//if err != nil {
	//	log.Fatal(err)
	//}
	//defer r.Close()
	//
	//
	//dstFile, err := os.Create("./test.zip")
	//
	//if err != nil{
	//	log.Println(err)
	//}
	//
	//r.WriteTo(dstFile)

	file, err := os.Open("")
	if err != nil {
		panic(err)
	}

	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = ','


	mobInfo := make([]MobileInfno, 0, 5)

	a := 0
	for {

		record, e := reader.Read()
		if e != nil {
			fmt.Println(e)
			break
		}

		number, err := strconv.ParseInt(record[0], 10, 64)
		if err!= nil{
			log.Println(err)
			continue
		}

		mnc, err := strconv.ParseInt(record[2], 10, 64)
		if err!= nil{
			log.Println(err)
		}

		region, err := strconv.Atoi(record[4])
		if err!= nil{
			log.Println(err)
		}


		dateTemp, err := time.Parse(time.RFC3339, record[5])


		if err != nil {
			log.Panic(err)
			continue
		}

		date := dateTemp.Format(layout)

		mInfo := MobileInfno{Number: number, OwnerId: record[1], MNC: mnc, Route: record[3], RegionCode: region, PortDate: date}

		mobInfo = append(mobInfo,mInfo)

		if (a % 100000==0){

			insertToBD(mobInfo, db)
			mobInfo = mobInfo[:0]

		}

		a+=1

	}

	fmt.Println(len(mobInfo))

}

func insertToBD(mobInfo []MobileInfno, db *sqlx.DB)  {
	tx,_ := db.Begin()
	stmp, _ := tx.Prepare("INSERT INTO MobileInfo (number, ownerId, mnc , route , regionCode, portDate ) VALUES (?, ?, ?, ?, ?, ?)")

	for _, post := range mobInfo{

		_, err := stmp.Exec(post.Number,post.OwnerId,post.MNC, post.Route, post.RegionCode, post.PortDate)
		if  err != nil {
			log.Println("err",err)
		}
	}

	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}
}
