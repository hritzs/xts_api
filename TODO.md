# TODO: Fix Hedge Monitor Issues

## Issues Identified
1. **Hedge Monitor Cannot Start**: Monitors fail to start due to missing `entry_timestamp` in database
2. **Socket Disconnection**: XTS Market Data Socket disconnects intermittently
3. **Delta-Neutral Building**: System is correctly building with delta-neutral quantities (PE/CE: 650 contracts each)

## Fixes Applied
- ✅ Added `entry_timestamp` column to `straddles` table in database migration
- ✅ Updated `insert_straddle` method to include `entry_timestamp` in columns and values
- ✅ Database will now properly store entry timestamps for new straddles

## Remaining Issues
- 🔄 Investigate socket disconnection causes (network/server issues?)
- 🔄 Test monitor initialization after database fix
- 🔄 Verify hedge/roll/SL monitors start correctly with entry_timestamp

## Next Steps
1. Restart application to apply database migration
2. Test straddle building and monitor initialization
3. Monitor socket stability and implement reconnection logic if needed
4. Verify delta-neutral calculations are working as expected
5. Create variable for price difference with ltp to place order currently it is 2 rupees