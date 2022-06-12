package hashedwheeltimer

type TimeTask interface {
	Run(timeout Timeout) bool
}
