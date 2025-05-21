"""util.py - Utility functions for the project."""


def encode_state_mask(mask: int) -> int:
    assert isinstance(mask, int), "mask must be int"
    # D1..D4 = mask bits 0..3
    D1 = (mask >> 0) & 1
    D2 = (mask >> 1) & 1
    D3 = (mask >> 2) & 1
    D4 = (mask >> 3) & 1
    P1 = D1 ^ D2 ^ D4
    P2 = D1 ^ D3 ^ D4
    P3 = D2 ^ D3 ^ D4
    P0 = P1 ^ P2 ^ D1 ^ P3 ^ D2 ^ D3 ^ D4  # overall parity
    return (
        (P0 << 7)
        | (D4 << 6)
        | (D3 << 5)
        | (D2 << 4)
        | (P3 << 3)
        | (D1 << 2)
        | (P2 << 1)
        | (P1 << 0)
    )


def decode_state_mask(code: int) -> int:
    assert isinstance(code, int), "code must be int"
    # Extract bits
    P1 = (code >> 0) & 1
    P2 = (code >> 1) & 1
    D1 = (code >> 2) & 1
    P3 = (code >> 3) & 1
    D2 = (code >> 4) & 1
    D3 = (code >> 5) & 1
    D4 = (code >> 6) & 1
    P0 = (code >> 7) & 1
    # Recompute parities
    cP1 = D1 ^ D2 ^ D4
    cP2 = D1 ^ D3 ^ D4
    cP3 = D2 ^ D3 ^ D4
    cP0 = P1 ^ P2 ^ D1 ^ P3 ^ D2 ^ D3 ^ D4
    # Syndrome
    syndrome = ((P1 != cP1) << 0) | ((P2 != cP2) << 1) | ((P3 != cP3) << 2)
    if cP0 != P0:
        # Double-bit error (uncorrectable)
        raise ValueError("Double-bit ECC error in state mask")
    if syndrome:
        # Single-bit error, correct
        code ^= 1 << syndrome
        # Re-extract after correction
        return decode_state_mask(code)
    # Return 4-bit mask
    return (D1 << 0) | (D2 << 1) | (D3 << 2) | (D4 << 3)
