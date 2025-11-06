# SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from multistorageclient.types import PatternType
from multistorageclient.utils import PatternMatcher


class TestPatternMatcher:
    """Test cases for the PatternMatcher class."""

    def test_no_patterns_includes_all(self):
        """Test that with no patterns, all files are included."""
        matcher = PatternMatcher([])
        assert matcher.should_include_file("file.txt") is True
        assert matcher.should_include_file("path/to/file.jpg") is True
        assert matcher.should_include_file("deep/nested/path/file.png") is True
        assert matcher.has_patterns() is False

    def test_exclude_patterns(self):
        """Test exclude patterns work correctly."""
        matcher = PatternMatcher([(PatternType.EXCLUDE, "*.tmp"), (PatternType.EXCLUDE, "*.log")])

        # Files matching exclude patterns should be excluded
        assert matcher.should_include_file("file.tmp") is False
        assert matcher.should_include_file("path/to/file.log") is False

        # Files not matching exclude patterns should be included
        assert matcher.should_include_file("file.txt") is True
        assert matcher.should_include_file("path/to/file.jpg") is True

        assert matcher.has_patterns() is True

    def test_include_patterns(self):
        """Test include patterns work correctly."""
        matcher = PatternMatcher([(PatternType.INCLUDE, "*.jpg"), (PatternType.INCLUDE, "*.png")])

        # Files matching include patterns should be included
        assert matcher.should_include_file("file.jpg") is True
        assert matcher.should_include_file("path/to/file.png") is True

        # Files not matching include patterns should be excluded
        assert matcher.should_include_file("file.txt") is False
        assert matcher.should_include_file("path/to/file.log") is False

        assert matcher.has_patterns() is True

    def test_exclude_then_include(self):
        """Test that include patterns can override exclude patterns."""
        matcher = PatternMatcher(
            [(PatternType.EXCLUDE, "*.tmp"), (PatternType.EXCLUDE, "*.log"), (PatternType.INCLUDE, "important.log")]
        )

        # Files matching exclude but not include should be excluded
        assert matcher.should_include_file("file.tmp") is False
        assert matcher.should_include_file("other.log") is False

        # Files matching include should be included even if they match exclude
        assert matcher.should_include_file("important.log") is True

        # Files matching neither should be included (default behavior)
        assert matcher.should_include_file("file.txt") is True

    def test_exclude_all_then_include_specific(self):
        """Test AWS S3 sync style: exclude all, then include specific types."""
        matcher = PatternMatcher(
            [(PatternType.EXCLUDE, "*"), (PatternType.INCLUDE, "*.jpg"), (PatternType.INCLUDE, "*.png")]
        )

        # Only files matching include patterns should be included
        assert matcher.should_include_file("file.jpg") is True
        assert matcher.should_include_file("path/to/file.png") is True

        # All other files should be excluded
        assert matcher.should_include_file("file.txt") is False
        assert matcher.should_include_file("file.tmp") is False
        assert matcher.should_include_file("path/to/file.log") is False

    def test_directory_patterns(self):
        """Test directory patterns (AWS S3 compatible, no ** support)."""
        matcher = PatternMatcher([(PatternType.INCLUDE, "images/*.jpg")])

        # Files in images directories should be included
        assert matcher.should_include_file("images/file.jpg") is True

        # Files not in images directories should be excluded
        assert matcher.should_include_file("file.jpg") is False
        assert matcher.should_include_file("path/to/images/file.jpg") is False
        assert matcher.should_include_file("images/file.png") is False

    def test_complex_directory_patterns(self):
        """Test complex directory patterns (AWS S3 compatible)."""
        matcher = PatternMatcher(
            [(PatternType.EXCLUDE, "temp/*"), (PatternType.EXCLUDE, "cache/*"), (PatternType.INCLUDE, "src/*.py")]
        )

        # Files in temp or cache directories should be excluded
        assert matcher.should_include_file("temp/file.txt") is False
        assert matcher.should_include_file("cache/file.txt") is False

        # Python files in src directories should be included
        assert matcher.should_include_file("src/file.py") is True

        # Non-Python files in src should be included (default behavior, not excluded by include pattern)
        assert matcher.should_include_file("src/file.txt") is True

        # Files in other directories should be included (default behavior)
        assert matcher.should_include_file("other/file.txt") is True

    def test_question_mark_patterns(self):
        """Test patterns with ? wildcard."""
        matcher = PatternMatcher([(PatternType.INCLUDE, "file?.txt")])

        # Files matching the pattern should be included
        assert matcher.should_include_file("file1.txt") is True
        assert matcher.should_include_file("filea.txt") is True

        # Files not matching the pattern should be excluded
        assert matcher.should_include_file("file.txt") is False
        assert matcher.should_include_file("file12.txt") is False
        assert matcher.should_include_file("other.txt") is False

    def test_character_class_patterns(self):
        """Test patterns with character classes."""
        matcher = PatternMatcher([(PatternType.INCLUDE, "file[0-9].txt")])

        # Files matching the character class should be included
        assert matcher.should_include_file("file0.txt") is True
        assert matcher.should_include_file("file5.txt") is True
        assert matcher.should_include_file("file9.txt") is True

        # Files not matching the character class should be excluded
        assert matcher.should_include_file("filea.txt") is False
        assert matcher.should_include_file("file.txt") is False
        assert matcher.should_include_file("file10.txt") is False

    def test_negated_character_class_patterns(self):
        """Test patterns with negated character classes."""
        matcher = PatternMatcher([(PatternType.INCLUDE, "file[!0-9].txt")])

        # Files not matching the character class should be included
        assert matcher.should_include_file("filea.txt") is True
        assert matcher.should_include_file("filez.txt") is True

        # Files matching the character class should be excluded
        assert matcher.should_include_file("file0.txt") is False
        assert matcher.should_include_file("file5.txt") is False
        assert matcher.should_include_file("file9.txt") is False

    def test_multiple_include_patterns(self):
        """Test multiple include patterns."""
        matcher = PatternMatcher(
            [(PatternType.INCLUDE, "*.jpg"), (PatternType.INCLUDE, "*.png"), (PatternType.INCLUDE, "*.gif")]
        )

        # Files matching any include pattern should be included
        assert matcher.should_include_file("file.jpg") is True
        assert matcher.should_include_file("file.png") is True
        assert matcher.should_include_file("file.gif") is True

        # Files not matching any include pattern should be excluded
        assert matcher.should_include_file("file.txt") is False
        assert matcher.should_include_file("file.bmp") is False

    def test_multiple_exclude_patterns(self):
        """Test multiple exclude patterns."""
        matcher = PatternMatcher(
            [(PatternType.EXCLUDE, "*.tmp"), (PatternType.EXCLUDE, "*.log"), (PatternType.EXCLUDE, "*.cache")]
        )

        # Files matching any exclude pattern should be excluded
        assert matcher.should_include_file("file.tmp") is False
        assert matcher.should_include_file("file.log") is False
        assert matcher.should_include_file("file.cache") is False

        # Files not matching any exclude pattern should be included
        assert matcher.should_include_file("file.txt") is True
        assert matcher.should_include_file("file.jpg") is True

    def test_repr(self):
        """Test string representation of PatternMatcher."""
        matcher = PatternMatcher(
            [(PatternType.INCLUDE, "*.jpg"), (PatternType.INCLUDE, "*.png"), (PatternType.EXCLUDE, "*.tmp")]
        )

        repr_str = repr(matcher)
        assert "PatternMatcher" in repr_str
        assert "patterns=" in repr_str
        assert "*.jpg" in repr_str
        assert "*.png" in repr_str
        assert "*.tmp" in repr_str
        assert "PatternType.EXCLUDE" in repr_str
        assert "PatternType.INCLUDE" in repr_str
