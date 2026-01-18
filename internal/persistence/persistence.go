package persistence

import (
    "encoding/json"
    "os"
)

type Persistence struct {
    FilePath string
}

func NewPersistence(filePath string) *Persistence {
    return &Persistence{FilePath: filePath}
}

func (p *Persistence) Save(data interface{}) error {
    file, err := os.Create(p.FilePath)
    if err != nil {
        return err
    }
    defer file.Close()

    encoder := json.NewEncoder(file)
    return encoder.Encode(data)
}

func (p *Persistence) Load(data interface{}) error {
    file, err := os.Open(p.FilePath)
    if err != nil {
        return err
    }
    defer file.Close()

    decoder := json.NewDecoder(file)
    return decoder.Decode(data)
}