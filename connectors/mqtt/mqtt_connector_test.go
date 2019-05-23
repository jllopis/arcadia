package mqtt

import (
	"testing"
)

func TestTopicMatches(t *testing.T) {
	t.Run("no wildcards", func(t *testing.T) {
		pattern := "/foo/bar/baz"
		topic := "/foo/bar/baz"

		expected := true
		result := topicMatches(pattern, topic)
		if result != expected {
			t.Errorf("Result is not correct. Expec.: {%v}.\nResult: {%v}", expected, result)
		}

		topic = "/foo/bar/baz/foo"

		expected = false
		result = topicMatches(pattern, topic)
		if result != expected {
			t.Errorf("Result is not correct. Expec.: {%v}.\nResult: {%v}", expected, result)
		}
	})
	t.Run("different topics", func(t *testing.T) {
		pattern := "/foo/baz/bar"
		topic := "/foo/bar/baz"

		expected := false
		result := topicMatches(pattern, topic)
		if result != expected {
			t.Errorf("Result is not correct. Expec.: {%v}.\nResult: {%v}", expected, result)
		}
	})
	t.Run("# at beginning", func(t *testing.T) {
		pattern := "#"
		topic := "/foo/bar/baz"

		expected := true
		result := topicMatches(pattern, topic)
		if result != expected {
			t.Errorf("Result is not correct. Expec.: {%v}.\nResult: {%v}", expected, result)
		}
	})
	t.Run("# at end", func(t *testing.T) {
		pattern := "/foo/#"
		topic := "/foo/bar/baz"

		expected := true
		result := topicMatches(pattern, topic)
		if result != expected {
			t.Errorf("Result is not correct. Expec.: {%v}.\nResult: {%v}", expected, result)
		}

		topic = "/"

		expected = false
		result = topicMatches(pattern, topic)
		if result != expected {
			t.Errorf("Result is not correct. Expec.: {%v}.\nResult: {%v}", expected, result)
		}
	})
	t.Run("# at end and topic has no children", func(t *testing.T) {
		pattern := "/foo/bar/#"
		topic := "/foo/bar"

		expected := true
		result := topicMatches(pattern, topic)
		if result != expected {
			t.Errorf("Result is not correct. Expec.: {%v}.\nResult: {%v}", expected, result)
		}

		topic = "/foo"

		expected = false
		result = topicMatches(pattern, topic)
		if result != expected {
			t.Errorf("Result is not correct. Expec.: {%v}.\nResult: {%v}", expected, result)
		}
	})
	t.Run("doesn't support # at beginning with children", func(t *testing.T) {
		pattern := "/#/bar/baz"
		topic := "/foo/bar/baz"

		expected := false
		result := topicMatches(pattern, topic)
		if result != expected {
			t.Errorf("Result is not correct. Expec.: {%v}.\nResult: {%v}", expected, result)
		}
	})
	t.Run("+ at beginning", func(t *testing.T) {
		pattern := "+/bar/baz"
		topic := "foo/bar/baz"

		expected := true
		result := topicMatches(pattern, topic)
		if result != expected {
			t.Errorf("Result is not correct. Expec.: {%v}.\nResult: {%v}", expected, result)
		}

		topic = "foo/foo/bar/baz"

		expected = false
		result = topicMatches(pattern, topic)
		if result != expected {
			t.Errorf("Result is not correct. Expec.: {%v}.\nResult: {%v}", expected, result)
		}
	})
	t.Run("+ in the middle", func(t *testing.T) {
		pattern := "foo/+/baz"
		topic := "foo/bar/baz"

		expected := true
		result := topicMatches(pattern, topic)
		if result != expected {
			t.Errorf("Result is not correct. Expec.: {%v}.\nResult: {%v}", expected, result)
		}

		topic = "foo/bar/foo/baz"

		expected = false
		result = topicMatches(pattern, topic)
		if result != expected {
			t.Errorf("Result is not correct. Expec.: {%v}.\nResult: {%v}", expected, result)
		}
	})
	t.Run("multiple wildcards", func(t *testing.T) {
		pattern := "foo/+/#"
		topic := "foo/bar/baz"

		expected := true
		result := topicMatches(pattern, topic)
		if result != expected {
			t.Errorf("Result is not correct. Expec.: {%v}.\nResult: {%v}", expected, result)
		}
	})
}
