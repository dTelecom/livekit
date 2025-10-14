package utils

import "sort"

func StringSlicesEqualIgnoreOrder(a, b []string) bool {
    if len(a) != len(b) {
        return false
    }
    sort.Strings(a)
    sort.Strings(b)

    for i := range a {
        if a[i] != b[i] {
            return false
        }
    }
    return true
}
