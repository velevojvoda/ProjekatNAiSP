package sstable

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
)

// Summary fajl ima format:
//   HEADER: [MinKeyLen: 4B][MaxKeyLen: 4B][SummaryStep: 4B][MinKey bytes][MaxKey bytes]
//   ULAZI:  za svaki upisani entry: [KeyLen: 4B][IndexOffset: 8B][Key bytes]
//
// Pri čitanju ne učitavamo ceo summary u memoriju — idemo entry-by-entry kroz
// fajl i tražimo poslednji ulaz čiji je ključ <= traženi ključ.

func writeSummaryFile(path string, indexEntries []SummaryEntry, step int) (SummaryHeader, error) {
	if step <= 0 {
		step = 1
	}

	f, err := os.Create(path)
	if err != nil {
		return SummaryHeader{}, err
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	defer w.Flush()

	header := SummaryHeader{SummaryStep: step}
	if len(indexEntries) > 0 {
		header.MinKey = indexEntries[0].Key
		header.MaxKey = indexEntries[len(indexEntries)-1].Key
	}

	if err := writeSummaryHeader(w, header); err != nil {
		return SummaryHeader{}, err
	}

	for i, entry := range indexEntries {
		if i%step != 0 && i != len(indexEntries)-1 {
			continue
		}
		if err := writeSummaryEntry(w, entry); err != nil {
			return SummaryHeader{}, err
		}
	}

	return header, nil
}

func writeSummaryHeader(w *bufio.Writer, h SummaryHeader) error {
	minBytes := []byte(h.MinKey)
	maxBytes := []byte(h.MaxKey)

	buf := make([]byte, 12+len(minBytes)+len(maxBytes))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(minBytes)))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(len(maxBytes)))
	binary.LittleEndian.PutUint32(buf[8:12], uint32(h.SummaryStep))
	copy(buf[12:12+len(minBytes)], minBytes)
	copy(buf[12+len(minBytes):], maxBytes)

	_, err := w.Write(buf)
	return err
}

func writeSummaryEntry(w *bufio.Writer, entry SummaryEntry) error {
	keyBytes := []byte(entry.Key)
	buf := make([]byte, 12+len(keyBytes))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(keyBytes)))
	binary.LittleEndian.PutUint64(buf[4:12], uint64(entry.IndexOffset))
	copy(buf[12:], keyBytes)
	_, err := w.Write(buf)
	return err
}

// readSummaryHeader pročita header iz otvorenog fajla i ostavi file cursor
// odmah posle headera (na prvom summary entry-ju).
func readSummaryHeader(f *os.File) (SummaryHeader, error) {
	var hbuf [12]byte
	if _, err := io.ReadFull(f, hbuf[:]); err != nil {
		return SummaryHeader{}, err
	}

	minLen := binary.LittleEndian.Uint32(hbuf[0:4])
	maxLen := binary.LittleEndian.Uint32(hbuf[4:8])
	step := binary.LittleEndian.Uint32(hbuf[8:12])

	keyBytes := make([]byte, int(minLen)+int(maxLen))
	if _, err := io.ReadFull(f, keyBytes); err != nil {
		return SummaryHeader{}, err
	}

	return SummaryHeader{
		MinKey:      string(keyBytes[:minLen]),
		MaxKey:      string(keyBytes[minLen:]),
		SummaryStep: int(step),
	}, nil
}

// readNextSummaryEntry pročita JEDAN ulaz sa trenutne pozicije fajla.
// Vraća io.EOF kada nema više ulaza.
func readNextSummaryEntry(f *os.File) (SummaryEntry, error) {
	var eh [12]byte
	if _, err := io.ReadFull(f, eh[:]); err != nil {
		return SummaryEntry{}, err
	}

	keyLen := binary.LittleEndian.Uint32(eh[0:4])
	indexOffset := binary.LittleEndian.Uint64(eh[4:12])

	kb := make([]byte, keyLen)
	if _, err := io.ReadFull(f, kb); err != nil {
		return SummaryEntry{}, err
	}

	return SummaryEntry{Key: string(kb), IndexOffset: int64(indexOffset)}, nil
}

// findIndexStartOffsetLazy traverzira summary fajl entry-by-entry (BEZ učitavanja
// celog fajla u memoriju) i vraća IndexOffset poslednjeg ulaza čiji je ključ
// <= traženi ključ. To je startna pozicija za sekvencijalno pretraživanje
// index.db fajla.
func findIndexStartOffsetLazy(path string, key string) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	header, err := readSummaryHeader(f)
	if err != nil {
		return 0, err
	}

	// Prazna tabela
	if header.MinKey == "" && header.MaxKey == "" {
		return 0, ErrNotFound
	}
	// Ključ van granica → sigurno ne postoji u ovoj tabeli
	if key < header.MinKey || key > header.MaxKey {
		return 0, ErrInvalidSummaryRange
	}

	var candidate int64 = -1
	for {
		entry, err := readNextSummaryEntry(f)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return 0, err
		}

		if entry.Key <= key {
			candidate = entry.IndexOffset
			continue
		}
		// Prešli smo ključ — više nema smisla čitati
		break
	}

	if candidate == -1 {
		return 0, ErrNotFound
	}
	return candidate, nil
}
