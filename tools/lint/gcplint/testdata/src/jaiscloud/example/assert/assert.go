package assert

func bad(m map[string]any) string {
	return m["k"].(string) // want `\[unsafe-assert\]`
}

func good(m map[string]any) (string, bool) {
	s, ok := m["k"].(string)
	return s, ok
}
