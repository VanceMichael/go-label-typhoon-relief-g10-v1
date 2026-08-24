package domain

import "time"

type Organization struct {
	ID, Name, Kind string
	Active         bool
	CreatedAt      time.Time
}
type User struct {
	ID, OrganizationID, Email, Role string
	Active                          bool
	CreatedAt                       time.Time
}
type Session struct {
	ID, UserID, TokenHash           string
	ExpiresAt, RevokedAt, CreatedAt time.Time
}
type StormEvent struct {
	ID, OrganizationID, Name, Severity string
	Status                             StormStatus
	Version                            int
	OpenedAt, ClosedAt                 time.Time
}
type Forecast struct {
	ID, StormID, Source, WarningLevel string
	Version                           int
	WindKPH                           float64
	ValidUntil, PublishedAt           time.Time
}
type RiskZone struct {
	ID, StormID, Name, RiskLevel string
	Confirmed                    bool
}
type Shelter struct {
	ID, OrganizationID, Name, Address string
	Status                            ShelterStatus
	Capacity, Reserved, Version       int
}
type Reservation struct {
	ID, ShelterID, HouseholdID string
	People                     int
	Status                     string
	ExpiresAt, CreatedAt       time.Time
}
type EvacuationOrder struct {
	ID, StormID, ZoneID      string
	Status                   OrderStatus
	Version                  int
	PublishedAt, CancelledAt time.Time
}
type Household struct {
	ID, ZoneID, HeadName string
	People               int
	Status               HouseholdStatus
	ShelterID            string
	ConfirmedAt          time.Time
}
type DispatchTask struct {
	ID, StormID, ZoneID, Title, OwnerID, Report string
	Status                                      TaskStatus
	Priority, Version                           int
	LeaseUntil, CreatedAt, CompletedAt          time.Time
}
type SupplyItem struct {
	ID, OrganizationID, SKU, Name string
	Quantity, Reserved, Version   int
}
type SupplyMovement struct {
	ID, ItemID, StormID, FromOrgID, ShelterID string
	Quantity                                  int
	Status                                    MovementStatus
	CreatedAt, DeliveredAt                    time.Time
}
type Alert struct {
	ID, StormID, OrderID, Channel, Status, LastError string
	Attempts                                         int
	NextAttemptAt, DeliveredAt                       time.Time
}
type OutboxEvent struct {
	ID, AggregateType, AggregateID, EventType, Payload, Status, LeaseOwner, LastError string
	Attempts                                                                          int
	AvailableAt, LeaseUntil, CreatedAt                                                time.Time
}
type AuditEvent struct {
	ID, OrganizationID, ActorID, ObjectType, ObjectID, Action, Result, RequestID, Detail string
	CreatedAt                                                                            time.Time
}
type Page[T any] struct {
	Items                []T
	Total, Limit, Offset int
}
type StormOverview struct {
	Storm                 StormEvent
	Forecasts             int
	Zones                 int
	OpenOrders            int
	OccupiedShelterPlaces int
	OpenTasks             int
	InTransitMovements    int
}
