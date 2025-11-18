#!/bin/bash
# Run validation on candidate tables which should check for duplicate atlas_person_ids
export TABLE_PREFIXES="candidates"
npm run qa:quiet 