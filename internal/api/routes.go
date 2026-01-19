package api

import (
	"github.com/gin-gonic/gin"
)

func SetupRoutes(router *gin.Engine) {
	router.POST("/search", HandleVectorSearch)
	router.POST("/upsert", HandleVectorUpsert)
}
