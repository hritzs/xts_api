"""
delta_neutral_utils.py - Delta-Neutral Position Calculation

OVERVIEW:
    Calculates unequal PE/CE quantities to achieve delta-neutral positioning.
    
LOGIC:
    - Input: Baseline contracts per leg (e.g., 300)
    - Output: Delta-adjusted quantities (e.g., 375 PE + 300 CE = 675 total)
    - Goal: Net portfolio delta ≈ 0
    
EXAMPLE:
    Input: 300 (baseline)
    CE Delta: 0.6, PE Delta: -0.4
    → Total to distribute: 600
    → CE gets 40% weight: 600 * 0.4 = 240 → rounds to 225 (3 lots)
    → PE gets 60% weight: 600 * 0.6 = 360 → rounds to 375 (5 lots)
    → Result: 225 CE + 375 PE = 600 total (delta-neutral)
"""

import logging
import math
from typing import Tuple

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# ==============================================================================
# CONSTANTS
# ==============================================================================
NIFTY_LOT_SIZE = 75  # NIFTY lot size (contracts per lot)

# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================
def mround(value: float, multiple: int) -> int:
    """
    Round value to nearest multiple using standard rounding (not ceiling).
    
    Args:
        value: The value to round (can be float)
        multiple: The multiple to round to (must be > 0)
    
    Returns:
        Integer rounded to nearest multiple
    
    Examples:
        mround(100, 75) = 75   (100/75 = 1.33 → rounds to 1 → 1*75 = 75)
        mround(150, 75) = 150  (150/75 = 2.00 → rounds to 2 → 2*75 = 150)
        mround(151, 75) = 150  (151/75 = 2.01 → rounds to 2 → 2*75 = 150)
        mround(188, 75) = 225  (188/75 = 2.51 → rounds to 3 → 3*75 = 225)
    """
    if multiple <= 0:
        raise ValueError(f"multiple must be > 0, got {multiple}")
    
    return int(round(float(value) / float(multiple)) * multiple)

# ==============================================================================
# MAIN CALCULATION FUNCTION
# ==============================================================================
def calculate_delta_neutral_quantities(
    ce_option_delta: float,
    pe_option_delta: float,
    target_contracts: int,
    lotsize: int = NIFTY_LOT_SIZE
) -> Tuple[int, int, int, int, float]:
    """
    Calculate delta-neutral PE and CE quantities given user input.
    
    Args:
        ce_option_delta: Delta of the CE option (e.g., 0.6 for ITM CE)
        pe_option_delta: Delta of the PE option (e.g., -0.4 for ITM PE)
        target_contracts: Baseline contracts per leg (e.g., 300)
        lotsize: Contracts per lot (default 75)
    
    Returns:
        Tuple[int, int, int, int, float]:
            - pe_lots: Number of PE lots to execute
            - ce_lots: Number of CE lots to execute
            - pe_contracts: Total PE contracts (pe_lots * 75)
            - ce_contracts: Total CE contracts (ce_lots * 75)
            - net_delta: Net portfolio delta after execution
    
    Logic:
        1. Total to distribute = target_contracts * 2 (e.g., 300 * 2 = 600)
        2. Calculate delta weights (reversed for neutrality)
        3. Allocate contracts according to weights
        4. Round to nearest lot size (75) using standard rounding
        5. Calculate net delta to verify neutrality
    
    Example:
        Input: target_contracts=300, ce_delta=0.6, pe_delta=-0.4
        
        Step 1: total = 300 * 2 = 600
        Step 2: weights = (PE:0.6, CE:0.4) [reversed]
        Step 3: PE_theoretical = 600 * 0.6 = 360
                CE_theoretical = 600 * 0.4 = 240
        Step 4: PE = mround(360, 75) = 375 (5 lots) [360/75=4.8→5]
                CE = mround(240, 75) = 225 (3 lots) [240/75=3.2→3]
        Step 5: net_delta = (375 * -0.4) + (225 * 0.6) = -150 + 135 = -15
        
        Result: (5, 3, 375, 225, -15.0)
    """
    try:
        # ======================================================================
        # STEP 1: LOG INPUTS AND VALIDATE
        # ======================================================================
        logger.info("=" * 80)
        logger.info("🔢 DELTA-NEUTRAL QUANTITY CALCULATION")
        logger.info("=" * 80)
        
        # Validation
        if target_contracts <= 0:
            raise ValueError(f"target_contracts must be > 0, got {target_contracts}")
        
        if lotsize <= 0:
            raise ValueError(f"lotsize must be > 0, got {lotsize}")
        
        # ======================================================================
        # STEP 2: CALCULATE TOTAL TO DISTRIBUTE
        # ======================================================================
        # User inputs baseline per leg (e.g., 300)
        # Total to distribute = 300 * 2 = 600 contracts
        total_contracts = target_contracts * 2
        
        logger.info(f"📊 Inputs:")
        logger.info(f"   CE option delta (raw): {ce_option_delta:.6f}")
        logger.info(f"   PE option delta (raw): {pe_option_delta:.6f}")
        logger.info(f"   Baseline per leg: {target_contracts}")
        logger.info(f"   Total to distribute: {total_contracts} contracts")
        logger.info(f"   Lot size: {lotsize}")
        
        # ======================================================================
        # STEP 3: CALCULATE DELTA WEIGHTS
        # ======================================================================
        # Use absolute values for weighting
        abs_ce_delta = abs(float(ce_option_delta))
        abs_pe_delta = abs(float(pe_option_delta))
        
        total_delta_weight = abs_ce_delta + abs_pe_delta
        
        # Handle zero delta case (fallback to equal split)
        if total_delta_weight == 0 or math.isclose(total_delta_weight, 0.0, rel_tol=1e-9):
            logger.warning("⚠ Total delta weight is zero, using equal 50/50 split")
            pe_weight = 0.5
            ce_weight = 0.5
        else:
            # Delta-neutral logic: REVERSE the weights
            # PE gets CE's delta weight, CE gets PE's delta weight
            # This ensures the larger delta leg gets fewer contracts
            pe_weight = abs_ce_delta / total_delta_weight
            ce_weight = abs_pe_delta / total_delta_weight
        
        logger.info(f"⚖️ Delta Weights:")
        logger.info(f"   PE weight (uses CE delta): {pe_weight:.6f} ({pe_weight*100:.2f}%)")
        logger.info(f"   CE weight (uses PE delta): {ce_weight:.6f} ({ce_weight*100:.2f}%)")
        logger.info(f"   Sum: {pe_weight + ce_weight:.6f}")
        
        # ======================================================================
        # STEP 4: ALLOCATE CONTRACTS BASED ON WEIGHTS
        # ======================================================================
        # Distribute the FULL total_contracts according to weights
        theoretical_pe_contracts = total_contracts * pe_weight
        theoretical_ce_contracts = total_contracts * ce_weight
        
        logger.info(f"📐 Theoretical (before rounding to lot size):")
        logger.info(f"   PE contracts: {theoretical_pe_contracts:.2f}")
        logger.info(f"   CE contracts: {theoretical_ce_contracts:.2f}")
        logger.info(f"   Sum: {theoretical_pe_contracts + theoretical_ce_contracts:.2f}")
        
        # ======================================================================
        # STEP 5: ROUND TO NEAREST LOT SIZE (STANDARD ROUNDING, NOT CEILING)
        # ======================================================================
        # Use standard rounding to nearest multiple of lotsize
        pe_contracts = mround(theoretical_pe_contracts, lotsize)
        ce_contracts = mround(theoretical_ce_contracts, lotsize)
        
        # Convert to lots
        pe_lots = max(1, int(pe_contracts / lotsize))
        ce_lots = max(1, int(ce_contracts / lotsize))
        
        # Recompute contracts from lots (ensures strict multiples)
        pe_contracts = pe_lots * lotsize
        ce_contracts = ce_lots * lotsize
        
        total_final_contracts = pe_contracts + ce_contracts
        
        logger.info(f"🎯 Final Quantities (after rounding to {lotsize}):")
        logger.info(f"   PE: {pe_contracts} contracts ({pe_lots} lots)")
        logger.info(f"   CE: {ce_contracts} contracts ({ce_lots} lots)")
        logger.info(f"   Total: {total_final_contracts} contracts ({pe_lots + ce_lots} lots)")
        
        # ======================================================================
        # STEP 6: CALCULATE NET DELTA
        # ======================================================================
        # For SHORT straddles:
        # - Short PE contributes delta based on PE's sign (usually negative, becomes positive when short)
        # - Short CE contributes delta based on CE's sign (usually positive, becomes negative when short)
        
        # When shorting options, flip the sign:
        # - Short PE with delta=-0.4 → contributes +0.4 per contract
        # - Short CE with delta=+0.6 → contributes -0.6 per contract
        pe_pos_delta = pe_contracts * (-1 * pe_option_delta)  # Flip sign for short
        ce_pos_delta = ce_contracts * (-1 * ce_option_delta)  # Flip sign for short
        net_delta = pe_pos_delta + ce_pos_delta
        
        logger.info(f"🎲 Portfolio Delta Analysis (SHORT straddle):")
        logger.info(f"   SHORT {pe_contracts} PE @delta={pe_option_delta:.6f}")
        logger.info(f"     → Contributes: {pe_pos_delta:.4f}")
        logger.info(f"   SHORT {ce_contracts} CE @delta={ce_option_delta:.6f}")
        logger.info(f"     → Contributes: {ce_pos_delta:.4f}")
        logger.info(f"   NET DELTA: {net_delta:.4f}")
        
        # ======================================================================
        # STEP 7: VERIFY DELTA NEUTRALITY
        # ======================================================================
        delta_neutral_threshold = lotsize * max(abs_ce_delta, abs_pe_delta)
        
        if abs(net_delta) < delta_neutral_threshold:
            logger.info(f"✅ DELTA NEUTRAL: |{net_delta:.2f}| < {delta_neutral_threshold:.2f}")
        else:
            logger.info(f"⚠️ DELTA IMBALANCE: |{net_delta:.2f}| >= {delta_neutral_threshold:.2f}")
            logger.info(f"   May require hedging or adjustment")
        
        logger.info("=" * 80)
        
        # ======================================================================
        # STEP 8: RETURN RESULTS
        # ======================================================================
        return pe_lots, ce_lots, pe_contracts, ce_contracts, net_delta
        
    except ValueError as e:
        # Handle validation errors
        logger.error(f"❌ Validation error: {e}")
        raise
        
    except Exception as e:
        # ======================================================================
        # EXCEPTION HANDLER: FALLBACK TO EQUAL SPLIT
        # ======================================================================
        logger.exception(f"❌ Error calculating delta-neutral quantities: {e}")
        logger.warning("⚠️ Falling back to EQUAL SPLIT (50/50)")
        
        try:
            # For 300 input, fallback gives 4 lots each (300 PE + 300 CE = 600 total)
            baseline_lots = max(1, int(round(target_contracts / lotsize)))
        except:
            baseline_lots = 1
        
        fallback_pe_lots = baseline_lots
        fallback_ce_lots = baseline_lots
        fallback_pe_contracts = fallback_pe_lots * lotsize
        fallback_ce_contracts = fallback_ce_lots * lotsize
        
        logger.warning(f"⚠️ FALLBACK RESULT:")
        logger.warning(f"   PE: {fallback_pe_contracts} contracts ({fallback_pe_lots} lots)")
        logger.warning(f"   CE: {fallback_ce_contracts} contracts ({fallback_ce_lots} lots)")
        logger.warning(f"   Total: {fallback_pe_contracts + fallback_ce_contracts} contracts")
        logger.warning(f"   Net delta: 0.0 (assumed)")
        
        return (
            fallback_pe_lots, 
            fallback_ce_lots, 
            fallback_pe_contracts, 
            fallback_ce_contracts, 
            0.0
        )

# ==============================================================================
# UTILITY FUNCTIONS (OPTIONAL)
# ==============================================================================
def validate_delta_neutral_result(
    pe_contracts: int,
    ce_contracts: int,
    ce_delta: float,
    pe_delta: float,
    max_delta_threshold: float = 100.0
) -> bool:
    """
    Validate that the result is sufficiently delta-neutral.
    
    Args:
        pe_contracts: Number of PE contracts
        ce_contracts: Number of CE contracts
        ce_delta: CE option delta
        pe_delta: PE option delta
        max_delta_threshold: Maximum acceptable net delta
    
    Returns:
        True if delta-neutral (within threshold), False otherwise
    """
    # For short straddle: flip signs
    pe_pos_delta = pe_contracts * (-1 * pe_delta)
    ce_pos_delta = ce_contracts * (-1 * ce_delta)
    net_delta = pe_pos_delta + ce_pos_delta
    
    is_neutral = abs(net_delta) <= max_delta_threshold
    
    if not is_neutral:
        logger.warning(
            f"⚠️ Delta neutrality check FAILED: "
            f"|{net_delta:.2f}| > {max_delta_threshold}"
        )
    else:
        logger.info(
            f"✅ Delta neutrality check PASSED: "
            f"|{net_delta:.2f}| <= {max_delta_threshold}"
        )
    
    return is_neutral
