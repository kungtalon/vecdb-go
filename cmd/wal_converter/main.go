package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"

	"vecdb-go/internal/persistence"
)

func main() {
	inputFile := flag.String("input", "", "Input WAL file path (required)")
	outputFile := flag.String("output", "", "Output WAL file path (required)")
	outputFormat := flag.String("format", "text", "Output format: 'binary' or 'text'")
	flag.Parse()

	if *inputFile == "" || *outputFile == "" {
		fmt.Println("Usage: wal_converter -input <file> -output <file> [-format binary|text]")
		fmt.Println("\nConvert WAL files between binary and text formats")
		fmt.Println("\nExamples:")
		fmt.Println("  # Convert binary WAL to text for inspection")
		fmt.Println("  wal_converter -input data.wal -output data.txt -format text")
		fmt.Println("\n  # Convert text WAL back to binary")
		fmt.Println("  wal_converter -input data.txt -output data.wal -format binary")
		flag.PrintDefaults()
		os.Exit(1)
	}

	if *outputFormat != "binary" && *outputFormat != "text" {
		fmt.Printf("Error: format must be 'binary' or 'text', got '%s'\n", *outputFormat)
		os.Exit(1)
	}

	err := convertWAL(*inputFile, *outputFile, *outputFormat)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("✓ Successfully converted %s to %s (format: %s)\n", 
		*inputFile, *outputFile, *outputFormat)
}

func convertWAL(inputPath, outputPath, outputFormat string) error {
	// Open input file
	inputFile, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("failed to open input file: %w", err)
	}
	defer inputFile.Close()

	// Create output file
	outputFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outputFile.Close()

	// Try both decoders to auto-detect input format
	inputReader := bufio.NewReader(inputFile)
	records, err := readAllRecords(inputReader)
	if err != nil {
		return fmt.Errorf("failed to read input records: %w", err)
	}

	fmt.Printf("Read %d records from input file\n", len(records))

	// Create output encoder
	var outputEncoder persistence.WALEncoder
	if outputFormat == "binary" {
		outputEncoder = persistence.NewBinaryWALEncoder("v1")
	} else {
		outputEncoder = persistence.NewTextWALEncoder("v1")
	}

	// Write all records
	outputWriter := bufio.NewWriter(outputFile)
	for i, record := range records {
		if err := outputEncoder.EncodeRecord(outputWriter, &record); err != nil {
			return fmt.Errorf("failed to encode record %d: %w", i, err)
		}
	}

	if err := outputWriter.Flush(); err != nil {
		return fmt.Errorf("failed to flush output: %w", err)
	}

	return nil
}

func readAllRecords(reader *bufio.Reader) ([]persistence.WALRecord, error) {
	var records []persistence.WALRecord

	// Try binary decoder first
	binaryDecoder := persistence.NewBinaryWALEncoder("v1")
	for {
		record, err := binaryDecoder.DecodeRecord(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			// Binary failed, try text decoder
			return readAllRecordsText(reader)
		}
		records = append(records, *record)
	}

	return records, nil
}

func readAllRecordsText(reader *bufio.Reader) ([]persistence.WALRecord, error) {
	// Reopen the file for text reading since we already consumed some bytes
	// In a real implementation, you'd want to seek back or handle this better
	var records []persistence.WALRecord
	textDecoder := persistence.NewTextWALEncoder("v1")

	for {
		record, err := textDecoder.DecodeRecord(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to decode as both binary and text: %w", err)
		}
		records = append(records, *record)
	}

	return records, nil
}
