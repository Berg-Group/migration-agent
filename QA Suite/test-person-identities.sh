#!/bin/bash
# Run validation on the person_identities table which has boolean columns
export TABLE_PREFIXES="person_identities"
npm run qa:quiet 