package domain

type StormStatus string

const (
	StormMonitoring StormStatus = "monitoring"
	StormActive     StormStatus = "active"
	StormClosed     StormStatus = "closed"
)

func (s StormStatus) CanMoveTo(next StormStatus) bool {
	return (s == StormMonitoring && next == StormActive) || (s == StormActive && next == StormClosed)
}

type ShelterStatus string

const (
	ShelterOpen   ShelterStatus = "open"
	ShelterFull   ShelterStatus = "full"
	ShelterClosed ShelterStatus = "closed"
)

type OrderStatus string

const (
	OrderDraft     OrderStatus = "draft"
	OrderPublished OrderStatus = "published"
	OrderCancelled OrderStatus = "cancelled"
)

func (s OrderStatus) CanMoveTo(next OrderStatus) bool {
	return s == OrderDraft && next == OrderPublished || s == OrderPublished && next == OrderCancelled
}

type HouseholdStatus string

const (
	HouseholdPending   HouseholdStatus = "pending"
	HouseholdConfirmed HouseholdStatus = "confirmed"
	HouseholdSafe      HouseholdStatus = "safe"
)

type TaskStatus string

const (
	TaskQueued    TaskStatus = "queued"
	TaskClaimed   TaskStatus = "claimed"
	TaskReported  TaskStatus = "reported"
	TaskCancelled TaskStatus = "cancelled"
)

func (s TaskStatus) CanMoveTo(next TaskStatus) bool {
	return s == TaskQueued && next == TaskClaimed || s == TaskClaimed && next == TaskReported || s == TaskClaimed && next == TaskCancelled
}

type MovementStatus string

const (
	MovementInTransit MovementStatus = "in_transit"
	MovementDelivered MovementStatus = "delivered"
	MovementCancelled MovementStatus = "cancelled"
)

type UserRole string

const (
	RoleCommander UserRole = "commander"
	RoleShelter   UserRole = "shelter"
	RoleRescue    UserRole = "rescue"
	RoleWarehouse UserRole = "warehouse"
	RoleAuditor   UserRole = "auditor"
)

func (r UserRole) Can(action string) bool {
	switch r {
	case RoleCommander:
		return true
	case RoleShelter:
		return action == "shelter.manage" || action == "shelter.read"
	case RoleRescue:
		return action == "dispatch.claim" || action == "dispatch.report"
	case RoleWarehouse:
		return action == "supply.dispatch"
	case RoleAuditor:
		return action == "audit.read"
	}
	return false
}
