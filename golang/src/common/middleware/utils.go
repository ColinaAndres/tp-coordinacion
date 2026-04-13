package middleware

import (
	"time"

	"crypto/rand"
	"encoding/hex"
)

const (
	publishTimeout  = 5 * time.Second
	contentType     = "text/plain"
	defaultExchange = ""
)

func SimpleCryptoID(tamano int) string {
	//Alternativa para no usar uuid sin permiso de la catedra, no es
	//criptográficamente seguro pero es suficiente para generar un consumerTag único en el ejercicio.
	//Recibe el tamaño del ID a generar en bytes, y devuelve un string hexadecimal de ese tamaño.
	b := make([]byte, tamano)
	rand.Read(b)
	return hex.EncodeToString(b)
}
