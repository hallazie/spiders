-- LOAD CSV WITH HEADERS FROM "file:///entity.csv" AS line MERGE (e:entity{id:line.id, title:line.title})



load csv with headers from 'file:////entity_x.csv' as line merge (e:entity{icd_code:line.icd_code, icd_title:line.icd_title, foundation_id:line.foundation_id, definition:line.definition, only_parent:line.only_parent, coded_elsewhere:line.coded_elsewhere, inclusions:line.inclusions, exclusions:line.exclusions})

load csv with headers from 'file:////icd11_nodes.csv' as line merge (e:entity{icd_code:line.icd_code, icd_title:line.icd_title, foundation_id:line.foundation_id, definition:line.definition, only_parent:line.only_parent, coded_elsewhere:line.coded_elsewhere, inclusions:line.inclusions, exclusions:line.exclusions, pc_associated_with:line.pc_associated_with, pc_causing_condition:line.pc_causing_condition, pc_has_manifestation:line.pc_has_manifestation, pc_specific_anatomy:line.pc_specific_anatomy, pc_infections_agent:line.pc_infections_agent, pc_chemical_agents:line.pc_chemical_agents, pc_medication:line.pc_medication})

load csv with headers from 'file:////relation_x.csv' as line match (from:entity{foundation_id:line.parent}),(to:entity{foundation_id:line.child}) merge (from)-[:ParentOf {weight:1}]->(to)

-- display ------------------------------------------------------------------

load csv with headers from 'file:////dep_entity_cn.csv' as line merge (e:disp_entity{icd_code:line.icd_code, icd_title:line.icd_title, foundation_id:line.foundation_id, definition:line.definition, only_parent:line.only_parent, coded_elsewhere:line.coded_elsewhere, inclusions:line.inclusions, exclusions:line.exclusions, pc_associated_with:line.pc_associated_with, pc_causing_condition:line.pc_causing_condition, pc_has_manifestation:line.pc_has_manifestation, pc_specific_anatomy:line.pc_specific_anatomy, pc_infections_agent:line.pc_infections_agent, pc_chemical_agents:line.pc_chemical_agents, pc_medication:line.pc_medication})

load csv with headers from 'file:////dep_extension_cn.csv' as line merge (e:disp_extension{icd_code:line.icd_code, icd_title:line.icd_title, foundation_id:line.foundation_id, definition:line.definition, only_parent:line.only_parent, coded_elsewhere:line.coded_elsewhere, inclusions:line.inclusions, exclusions:line.exclusions, pc_associated_with:line.pc_associated_with, pc_causing_condition:line.pc_causing_condition, pc_has_manifestation:line.pc_has_manifestation, pc_specific_anatomy:line.pc_specific_anatomy, pc_infections_agent:line.pc_infections_agent, pc_chemical_agents:line.pc_chemical_agents, pc_medication:line.pc_medication})

load csv with headers from 'file:////relation_x.csv' as line match (from:disp_entity{foundation_id:line.parent}),(to:disp_entity{foundation_id:line.child}) merge (from)-[:ParentOf {weight:1}]->(to)

load csv with headers from 'file:////relation_x.csv' as line match (from:disp_entity{foundation_id:line.parent}),(to:disp_extension{icd_code:line.child}) merge (from)-[:PostCoordination {weight:1}]->(to)

start n = node(*) return n