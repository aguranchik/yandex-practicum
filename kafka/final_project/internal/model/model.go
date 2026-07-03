package model

import (
	"fmt"
	"strings"
	"time"
)

type Money struct {
	Amount   float64 `json:"amount"`
	Currency string  `json:"currency"`
}

type Stock struct {
	Available int `json:"available"`
	Reserved  int `json:"reserved"`
}

type Image struct {
	URL string `json:"url"`
	Alt string `json:"alt"`
}

type Product struct {
	ProductID      string            `json:"product_id"`
	Name           string            `json:"name"`
	Description    string            `json:"description"`
	Price          Money             `json:"price"`
	Category       string            `json:"category"`
	Brand          string            `json:"brand"`
	Stock          Stock             `json:"stock"`
	SKU            string            `json:"sku"`
	Tags           []string          `json:"tags"`
	Images         []Image           `json:"images"`
	Specifications map[string]string `json:"specifications"`
	CreatedAt      time.Time         `json:"created_at"`
	UpdatedAt      time.Time         `json:"updated_at"`
	Index          string            `json:"index"`
	StoreID        string            `json:"store_id"`
}

func (p Product) Validate() error {
	if strings.TrimSpace(p.ProductID) == "" {
		return fmt.Errorf("product_id is required")
	}
	if strings.TrimSpace(p.Name) == "" {
		return fmt.Errorf("name is required")
	}
	if p.Price.Amount < 0 {
		return fmt.Errorf("price.amount must not be negative")
	}
	if strings.TrimSpace(p.StoreID) == "" {
		return fmt.Errorf("store_id is required")
	}
	return nil
}

type ClientEvent struct {
	RequestID   string    `json:"request_id"`
	UserID      string    `json:"user_id"`
	RequestType string    `json:"request_type"`
	Query       string    `json:"query,omitempty"`
	RequestedAt time.Time `json:"requested_at"`
}

type BlacklistCommand struct {
	Action    string    `json:"action"`
	ProductID string    `json:"product_id"`
	Reason    string    `json:"reason,omitempty"`
	UpdatedAt time.Time `json:"updated_at"`
}

type BlacklistEntry struct {
	ProductID string    `json:"product_id"`
	Reason    string    `json:"reason"`
	BlockedAt time.Time `json:"blocked_at"`
}

type Recommendation struct {
	UserID      string    `json:"user_id"`
	ProductID   string    `json:"product_id"`
	ProductName string    `json:"product_name"`
	Reason      string    `json:"reason"`
	GeneratedAt time.Time `json:"generated_at"`
}
