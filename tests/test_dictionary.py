from scripts.dictionary import to_bitmask, DICTIONARY

def test_empty_tags():
    assert to_bitmask([]) == '0' * len(DICTIONARY)

def test_all_tags():
    assert to_bitmask(DICTIONARY) == '1' * len(DICTIONARY)

def test_single_tag():
    tag = DICTIONARY[5]
    expected = ['0'] * len(DICTIONARY)
    expected[5] = '1'
    assert to_bitmask([tag]) == ''.join(expected)

def test_invalid_tags_ignored():
    assert to_bitmask(["nonexistent", "neo-noir"]) == to_bitmask(["neo-noir"])

def test_duplicates_do_not_affect_output():
    assert to_bitmask(["neo-noir", "neo-noir"]) == to_bitmask(["neo-noir"])