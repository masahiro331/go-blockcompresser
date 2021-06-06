package main

import (
	"fmt"
	"io"
	"log"
	"os"

	"github.com/masahiro331/go-blockcompresser/compresser"
	"github.com/masahiro331/go-vmdk-parser/pkg/virtualization/vmdk"
)

func main() {

	f, err := os.Open(os.Args[2])
	if err != nil {
		log.Fatal(err)
	}

	switch os.Args[1] {
	case "parse":
		cf, err := compresser.NewCompressedFile(f)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%+v\n", cf.Header.Core)
		fmt.Println(len(cf.Header.CompressedTable))
	case "create":
		reader, err := vmdk.NewReader(f)
		if err != nil {
			log.Fatal(err)
		}

		for {
			partition, err := reader.Next()
			if err != nil {
				if err == io.EOF {
					break
				}
				log.Fatal(err)
			}

			if !partition.Bootable() {
				cf, err := compresser.Create(partition.Name(), 4096, partition.GetSize()*512)
				if err != nil {
					log.Fatal(err)
				}
				defer cf.Close()

				buf := make([]byte, 4096)
				for {
					_, err := reader.Read(buf)
					if err != nil {
						if err == io.EOF {
							break
						}
						log.Fatal(err)
					}

					_, err = cf.Write(buf)
					if err != nil {
						log.Fatal(err)
					}
				}
			}
		}
	}
}