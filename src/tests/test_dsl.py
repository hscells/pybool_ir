from hyperbool.query.parser import Parser


def test_basic():
    result = Parser.runTests('''
    # Single expression.
    (atom1 AND atom2)
    # Nested expression.
    ((atom1 OR atom2) AND (atom3 OR atom4))
    # Single atom in expression edge case.
    ((atom1) AND (atom2))
    # Triplet expression.
    (atom1 AND atom2 AND atom3)
    # Triplet expression with multiple operators.
    (atom1 AND atom2 OR atom3)
    # Phrase and term atoms.
    (atom1 AND "atom 2")
    # Field restrictions on atoms.
    (atom1[Title] AND atom2[Title/Abstract])
    ''')
    assert result[0]
