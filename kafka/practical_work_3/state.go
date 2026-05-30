package main

import (
	"sort"
	"strings"
)

func normalizeUserID(value string) string {
	return strings.TrimSpace(value)
}

func normalizeWord(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}

	return false
}

func addUnique(values []string, additions []string) []string {
	seen := make(map[string]struct{}, len(values)+len(additions))
	result := make([]string, 0, len(values)+len(additions))

	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}

	for _, value := range additions {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}

	sort.Strings(result)
	return result
}

func removeValues(values []string, removals []string) []string {
	removeSet := make(map[string]struct{}, len(removals))
	for _, value := range removals {
		value = strings.TrimSpace(value)
		if value != "" {
			removeSet[value] = struct{}{}
		}
	}

	result := make([]string, 0, len(values))
	for _, value := range values {
		if _, remove := removeSet[value]; remove {
			continue
		}
		result = append(result, value)
	}

	sort.Strings(result)
	return result
}
