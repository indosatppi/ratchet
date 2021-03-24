package processors

func createProcessorName(processor string, name string) string {
	if name != "" {
		return name + "(" + processor + ")"
	}

	return processor
}
