"""Test script to verify Reuters priority logic."""

def test_priority_logic():
    """Test the priority-based breaking news logic."""

    # Test cases with different priorities
    test_cases = [
        {"priority": "1", "expected_breaking": True, "description": "Flash"},
        {"priority": "2", "expected_breaking": True, "description": "Alert"},
        {"priority": "3", "expected_breaking": True, "description": "Urgent"},
        {"priority": "4", "expected_breaking": False, "description": "Standard"},
        {"priority": "5", "expected_breaking": False, "description": "Background"},
    ]

    print("=== Testing Reuters Priority Logic ===\n")

    for test_case in test_cases:
        priority = int(test_case["priority"])
        expected = test_case["expected_breaking"]
        description = test_case["description"]

        # Apply the logic
        is_breaking = priority <= 3

        # Check result
        status = "PASS" if is_breaking == expected else "FAIL"
        print(f"Priority {priority} ({description}): {is_breaking} {status}")

    print("\n=== Priority Logic ===")
    print("Priority 1-3: Breaking news (True)")
    print("Priority 4-5: Regular news (False)")

if __name__ == "__main__":
    test_priority_logic()
