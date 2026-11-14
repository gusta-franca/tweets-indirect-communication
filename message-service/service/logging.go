/*
   Data de criação: 14/11/2026
   Estudante: Gustavo Martins França
   Configurações de logging
*/

package main

import (
	"log"
	"os"
	"path/filepath"
)

// Cria o diretório e o arquivo de log e redireciona o pacote log para ele
func startLogging(path string) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(path), os.ModePerm); err != nil {
		return nil, err
	}

	logFile, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)

	if err != nil {
		return nil, err
	}

	log.SetOutput(logFile)

	return logFile, nil
}
