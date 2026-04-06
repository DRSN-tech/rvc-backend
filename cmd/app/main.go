package main

import (
	"os"

	"github.com/DRSN-tech/go-backend/internal/app"
	config "github.com/DRSN-tech/go-backend/internal/cfg"
	"github.com/DRSN-tech/go-backend/pkg/logger"
)

// @title			Retail Vision API
// @version		1.0
// @description	API сервис для распознавания товаров и управления каталогом.
// @host			localhost:8080
// @BasePath		/api/v1
func main() {
	log := logger.NewSlogLogger()

	cfg, err := config.Load()
	if err != nil {
		log.Errorf(err, "failed to load config")
		os.Exit(1)
	}

	application, err := app.NewApp(cfg, log)
	if err != nil {
		log.Errorf(err, "failed to initialize app")
		os.Exit(1)
	}

	if err := application.Run(); err != nil {
		os.Exit(1)
	}
}
