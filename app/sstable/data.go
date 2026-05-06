
package sstable

import (
    "bufio"
    "encoding/binary"
    "fmt"
    "hash/crc32"
    "io"
    "os"

    "ProjekatNAiSP/app/model"
)

func writeDataFile(path string, records []model.Record) ([]IndexEntry, [][]byte, error) {
    f, err := os.Create(path)
    if err != nil {
        return nil, nil, err
    }
    defer f.Close()

    w := bufio.NewWriter(f)
    defer w.Flush()

    var offset int64
    indexEntries := make([]IndexEntry, 0, len(records))
    values := make([][]byte, 0, len(records))

    for _, rec := range records {
        encoded, err := encodeDataRecord(rec)
        if err != nil {
            return nil, nil, err
        }
        if _, err := w.Write(encoded); err != nil {
            return nil, nil, err
        }
        indexEntries = append(indexEntries, IndexEntry{Key: rec.Key, DataOffset: offset})
        values = append(values, append([]byte(nil), rec.Value...))
        offset += int64(len(encoded))
    }
    return indexEntries, values, nil
}

func encodeDataRecord(rec model.Record) ([]byte, error) {
    keyBytes := []byte(rec.Key)
    valueBytes := rec.Value
    payloadLen := 1 + 4 + 4 + len(keyBytes) + len(valueBytes)
    buf := make([]byte, 4+payloadLen)

    if rec.Tombstone {
        buf[4] = 1
    }
    binary.LittleEndian.PutUint32(buf[5:9], uint32(len(keyBytes)))
    binary.LittleEndian.PutUint32(buf[9:13], uint32(len(valueBytes)))
    copy(buf[13:13+len(keyBytes)], keyBytes)
    copy(buf[13+len(keyBytes):], valueBytes)

    crc := crc32.ChecksumIEEE(buf[4:])
    binary.LittleEndian.PutUint32(buf[0:4], crc)
    return buf, nil
}

func decodeDataRecordAt(f *os.File, offset int64) (model.Record, int64, error) {
    if _, err := f.Seek(offset, io.SeekStart); err != nil {
        return model.Record{}, 0, err
    }

    var header [13]byte
    if _, err := io.ReadFull(f, header[:]); err != nil {
        return model.Record{}, 0, err
    }

    storedCRC := binary.LittleEndian.Uint32(header[0:4])
    tombstone := header[4] == 1
    keyLen := binary.LittleEndian.Uint32(header[5:9])
    valueLen := binary.LittleEndian.Uint32(header[9:13])

    payload := make([]byte, int(keyLen)+int(valueLen))
    if _, err := io.ReadFull(f, payload); err != nil {
        return model.Record{}, 0, err
    }

    crcBuf := make([]byte, 9+len(payload))
    copy(crcBuf[:9], header[4:13])
    copy(crcBuf[9:], payload)
    if crc32.ChecksumIEEE(crcBuf) != storedCRC {
        return model.Record{}, 0, fmt.Errorf("%w at data offset %d", ErrCorruptedData, offset)
    }

    key := string(payload[:keyLen])
    value := append([]byte(nil), payload[keyLen:]...)
    return model.Record{Key: key, Value: value, Tombstone: tombstone}, int64(13 + len(payload)), nil
}

func scanDataFile(path string, fn func(idx int, rec model.Record, offset int64) error) error {
    f, err := os.Open(path)
    if err != nil {
        return err
    }
    defer f.Close()

    var offset int64
    idx := 0
    for {
        rec, size, err := decodeDataRecordAt(f, offset)
        if err == io.EOF {
            return nil
        }
        if err == io.ErrUnexpectedEOF {
            return fmt.Errorf("%w at end of data file", ErrCorruptedData)
        }
        if err != nil {
            return err
        }
        if err := fn(idx, rec, offset); err != nil {
            return err
        }
        offset += size
        idx++
    }
}
