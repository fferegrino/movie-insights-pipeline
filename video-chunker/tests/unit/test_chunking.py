import pytest

from video_chunker.chunking import ChunkBoundary, calculate_chunk_boundaries


def boundaries_to_tuples(boundaries: list[ChunkBoundary]) -> list[tuple[float, float]]:
    return [(boundary.left, boundary.right) for boundary in boundaries]


class TestCalculateChunkBoundaries:
    """Test cases for calculate_chunk_boundaries function."""

    def test_basic_chunking_with_overlap(self):
        """Test basic chunking with overlap as shown in documentation example."""
        result = calculate_chunk_boundaries(10.0, 3.0, 0.5)
        expected = [(0.0, 3.5), (2.5, 6.5), (5.5, 9.5), (8.5, 10.0)]
        actual_boundaries = boundaries_to_tuples(result)
        assert actual_boundaries == expected

    def test_chunking_without_overlap(self):
        """Test chunking without overlap as shown in documentation example."""
        result = calculate_chunk_boundaries(5.0, 2.0, 0.0)
        expected = [(0.0, 2.0), (2.0, 4.0), (4.0, 5.0)]
        actual_boundaries = boundaries_to_tuples(result)
        assert actual_boundaries == expected

    def test_single_chunk_when_duration_less_than_chunk_duration(self):
        """Test when video duration is less than chunk duration."""
        result = calculate_chunk_boundaries(3.0, 5.0, 1.0)
        expected = [(0.0, 3.0)]
        actual_boundaries = boundaries_to_tuples(result)
        assert actual_boundaries == expected

    def test_exact_multiple_chunks(self):
        """Test when duration is exactly divisible by chunk duration."""
        result = calculate_chunk_boundaries(6.0, 2.0, 0.5)
        expected = [(0.0, 2.5), (1.5, 4.5), (3.5, 6.0)]
        actual_boundaries = boundaries_to_tuples(result)
        assert actual_boundaries == expected

    def test_fractional_duration(self):
        """Test with fractional duration values."""
        result = calculate_chunk_boundaries(7.5, 2.0, 0.25)
        expected = [(0.0, 2.25), (1.75, 4.25), (3.75, 6.25), (5.75, 7.5)]
        actual_boundaries = boundaries_to_tuples(result)
        assert actual_boundaries == expected

    def test_fractional_chunk_duration(self):
        """Test with fractional chunk duration."""
        result = calculate_chunk_boundaries(5.0, 1.5, 0.25)
        expected = [(0.0, 1.75), (1.25, 3.25), (2.75, 4.75), (4.25, 5.0)]
        actual_boundaries = boundaries_to_tuples(result)
        assert actual_boundaries == expected

    def test_zero_duration(self):
        """Test with zero duration."""
        result = calculate_chunk_boundaries(0.0, 2.0, 0.5)
        expected = []
        actual_boundaries = boundaries_to_tuples(result)
        assert actual_boundaries == expected

    def test_very_small_duration(self):
        """Test with very small duration."""
        result = calculate_chunk_boundaries(0.1, 1.0, 0.05)
        expected = [(0.0, 0.1)]
        actual_boundaries = boundaries_to_tuples(result)
        assert actual_boundaries == expected

    def test_large_overlap(self):
        """Test with large overlap (but still less than chunk duration)."""
        result = calculate_chunk_boundaries(10.0, 4.0, 2.0)
        expected = [(0.0, 6.0), (2.0, 10.0), (6.0, 10.0)]
        actual_boundaries = boundaries_to_tuples(result)
        assert actual_boundaries == expected

    def test_minimal_overlap(self):
        """Test with minimal overlap."""
        result = calculate_chunk_boundaries(6.0, 2.0, 0.001)
        expected = [(0.0, 2.001), (1.999, 4.001), (3.999, 6.0)]
        actual_boundaries = boundaries_to_tuples(result)
        assert actual_boundaries == expected


class TestCalculateChunkBoundariesErrors:
    """Test error cases for calculate_chunk_boundaries function."""

    def test_negative_duration(self):
        """Test that negative duration raises ValueError."""
        with pytest.raises(ValueError, match="Duration must be non-negative"):
            calculate_chunk_boundaries(-1.0, 2.0, 0.5)

    def test_zero_chunk_duration(self):
        """Test that zero chunk duration raises ValueError."""
        with pytest.raises(ValueError, match="Chunk duration must be positive"):
            calculate_chunk_boundaries(10.0, 0.0, 0.5)

    def test_negative_chunk_duration(self):
        """Test that negative chunk duration raises ValueError."""
        with pytest.raises(ValueError, match="Chunk duration must be positive"):
            calculate_chunk_boundaries(10.0, -2.0, 0.5)

    def test_negative_overlap(self):
        """Test that negative overlap raises ValueError."""
        with pytest.raises(ValueError, match="Overlap must be non-negative"):
            calculate_chunk_boundaries(10.0, 2.0, -0.5)

    def test_overlap_equal_to_chunk_duration(self):
        """Test that overlap equal to chunk duration raises ValueError."""
        with pytest.raises(ValueError, match="Overlap must be less than chunk duration"):
            calculate_chunk_boundaries(10.0, 2.0, 2.0)

    def test_overlap_greater_than_chunk_duration(self):
        """Test that overlap greater than chunk duration raises ValueError."""
        with pytest.raises(ValueError, match="Overlap must be less than chunk duration"):
            calculate_chunk_boundaries(10.0, 2.0, 3.0)


class TestCalculateChunkBoundariesEdgeCases:
    """Test edge cases for calculate_chunk_boundaries function."""

    def test_floating_point_precision(self):
        """Test that floating point precision is handled correctly."""
        result = calculate_chunk_boundaries(1.0, 0.3333333333333333, 0.1)
        # Should handle floating point arithmetic correctly
        assert len(result) > 0
        actual_boundaries = boundaries_to_tuples(result)
        assert all(isinstance(boundary[0], float) and isinstance(boundary[1], float) for boundary in actual_boundaries)

    def test_very_large_numbers(self):
        """Test with very large duration values."""
        result = calculate_chunk_boundaries(1000000.0, 1000.0, 100.0)
        assert len(result) == 1000
        actual_boundaries = boundaries_to_tuples(result)
        assert actual_boundaries[0][0] == 0.0
        assert actual_boundaries[-1][1] == 1000000.0

    def test_chunk_boundaries_are_ordered(self):
        """Test that chunk boundaries are properly ordered."""
        result = calculate_chunk_boundaries(10.0, 2.0, 0.5)
        actual_boundaries = boundaries_to_tuples(result)
        for i in range(len(actual_boundaries) - 1):
            assert actual_boundaries[i][1] >= actual_boundaries[i][0]  # End time >= start time
            assert actual_boundaries[i][1] <= actual_boundaries[i + 1][1]  # End times are non-decreasing

    def test_no_gaps_between_chunks(self):
        """Test that there are no gaps between consecutive chunks."""
        result = calculate_chunk_boundaries(10.0, 2.0, 0.5)
        actual_boundaries = boundaries_to_tuples(result)
        for i in range(len(actual_boundaries) - 1):
            # Each chunk should overlap with the next one
            assert actual_boundaries[i][1] >= actual_boundaries[i + 1][0]

    def test_first_chunk_starts_at_zero(self):
        """Test that the first chunk always starts at 0."""
        result = calculate_chunk_boundaries(10.0, 2.0, 0.5)
        actual_boundaries = boundaries_to_tuples(result)
        assert actual_boundaries[0][0] == 0.0

    def test_last_chunk_ends_at_duration(self):
        """Test that the last chunk ends at the total duration."""
        result = calculate_chunk_boundaries(10.0, 2.0, 0.5)
        actual_boundaries = boundaries_to_tuples(result)
        assert actual_boundaries[-1][1] == 10.0
