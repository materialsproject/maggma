# Changelog

## [v0.24.0](https://github.com/materialsproject/maggma/tree/v0.24.0) (2020-11-02)

[Full Changelog](https://github.com/materialsproject/maggma/compare/v0.23.3...v0.24.0)

**Merged pull requests:**

- Small fix to make sure searchable\_fields are updated [\#303](https://github.com/materialsproject/maggma/pull/303) ([jmmshn](https://github.com/jmmshn))

## [v0.23.3](https://github.com/materialsproject/maggma/tree/v0.23.3) (2020-09-23)

[Full Changelog](https://github.com/materialsproject/maggma/compare/v0.23.2...v0.23.3)

## [v0.23.2](https://github.com/materialsproject/maggma/tree/v0.23.2) (2020-09-23)

[Full Changelog](https://github.com/materialsproject/maggma/compare/v0.23.1...v0.23.2)

## [v0.23.1](https://github.com/materialsproject/maggma/tree/v0.23.1) (2020-09-21)

[Full Changelog](https://github.com/materialsproject/maggma/compare/v0.23.0...v0.23.1)

**Closed issues:**

- FEATURE: Python file runner [\#277](https://github.com/materialsproject/maggma/issues/277)

## [v0.23.0](https://github.com/materialsproject/maggma/tree/v0.23.0) (2020-09-14)

[Full Changelog](https://github.com/materialsproject/maggma/compare/v0.22.3...v0.23.0)

**Closed issues:**

- Separate out S3 Object reference keys from search keys [\#206](https://github.com/materialsproject/maggma/issues/206)

**Merged pull requests:**

- Add custom source loading [\#278](https://github.com/materialsproject/maggma/pull/278) ([shyamd](https://github.com/shyamd))
- Inject metadata via fields rather than by indicies [\#265](https://github.com/materialsproject/maggma/pull/265) ([shyamd](https://github.com/shyamd))

## [v0.22.3](https://github.com/materialsproject/maggma/tree/v0.22.3) (2020-08-26)

[Full Changelog](https://github.com/materialsproject/maggma/compare/v0.22.2...v0.22.3)

**Merged pull requests:**

- added context manager for stores [\#258](https://github.com/materialsproject/maggma/pull/258) ([jmmshn](https://github.com/jmmshn))

## [v0.22.2](https://github.com/materialsproject/maggma/tree/v0.22.2) (2020-08-21)

[Full Changelog](https://github.com/materialsproject/maggma/compare/v0.22.1...v0.22.2)

**Merged pull requests:**

- Minor bug fixes to S3Store [\#253](https://github.com/materialsproject/maggma/pull/253) ([jmmshn](https://github.com/jmmshn))

## [v0.22.1](https://github.com/materialsproject/maggma/tree/v0.22.1) (2020-08-11)

[Full Changelog](https://github.com/materialsproject/maggma/compare/v0.22.0...v0.22.1)

**Merged pull requests:**

- accept int as sort keys instead of Sort\(\) in .query\(\) and .groupby\(\) [\#241](https://github.com/materialsproject/maggma/pull/241) ([rkingsbury](https://github.com/rkingsbury))
- Update setup.py [\#225](https://github.com/materialsproject/maggma/pull/225) ([jmmshn](https://github.com/jmmshn))

## [v0.22.0](https://github.com/materialsproject/maggma/tree/v0.22.0) (2020-07-16)

[Full Changelog](https://github.com/materialsproject/maggma/compare/v0.21.0...v0.22.0)

**Merged pull requests:**

- Ensure Metadata in Documents from GridFS [\#222](https://github.com/materialsproject/maggma/pull/222) ([shyamd](https://github.com/shyamd))
- Projection\_Builder tests [\#213](https://github.com/materialsproject/maggma/pull/213) ([acrutt](https://github.com/acrutt))
- \[WIP\] Proper multithreading and msgpack fix [\#205](https://github.com/materialsproject/maggma/pull/205) ([jmmshn](https://github.com/jmmshn))
- Fix projection\_builder.update\_targets\(\) [\#179](https://github.com/materialsproject/maggma/pull/179) ([acrutt](https://github.com/acrutt))

## [v0.21.0](https://github.com/materialsproject/maggma/tree/v0.21.0) (2020-06-22)

[Full Changelog](https://github.com/materialsproject/maggma/compare/v0.20.0...v0.21.0)

**Merged pull requests:**

- Reconstruct metadata from index in S3 Store [\#182](https://github.com/materialsproject/maggma/pull/182) ([jmmshn](https://github.com/jmmshn))
- MapBuilder retry\_failed Fix [\#180](https://github.com/materialsproject/maggma/pull/180) ([acrutt](https://github.com/acrutt))
- MapBuilder retry\_failed bug [\#111](https://github.com/materialsproject/maggma/pull/111) ([acrutt](https://github.com/acrutt))

## [v0.20.0](https://github.com/materialsproject/maggma/tree/v0.20.0) (2020-05-02)

[Full Changelog](https://github.com/materialsproject/maggma/compare/v0.19.1...v0.20.0)

**Merged pull requests:**

- Initial Drone Implementation [\#145](https://github.com/materialsproject/maggma/pull/145) ([wuxiaohua1011](https://github.com/wuxiaohua1011))
- parallel s3 store wrting [\#130](https://github.com/materialsproject/maggma/pull/130) ([jmmshn](https://github.com/jmmshn))
- Make GridFSStore query check files store first. [\#128](https://github.com/materialsproject/maggma/pull/128) ([munrojm](https://github.com/munrojm))

## [v0.19.1](https://github.com/materialsproject/maggma/tree/v0.19.1) (2020-04-06)

[Full Changelog](https://github.com/materialsproject/maggma/compare/v0.19.0...v0.19.1)

## [v0.19.0](https://github.com/materialsproject/maggma/tree/v0.19.0) (2020-04-06)

[Full Changelog](https://github.com/materialsproject/maggma/compare/v0.18.0...v0.19.0)

**Closed issues:**

- ISSUE: newer\_in method incompatible with GridFSStore [\#113](https://github.com/materialsproject/maggma/issues/113)

**Merged pull requests:**

- Fix async [\#129](https://github.com/materialsproject/maggma/pull/129) ([shyamd](https://github.com/shyamd))
- small fixes [\#115](https://github.com/materialsproject/maggma/pull/115) ([jmmshn](https://github.com/jmmshn))
- Store updates [\#114](https://github.com/materialsproject/maggma/pull/114) ([jmmshn](https://github.com/jmmshn))
- \[WIP\] Add EndpointCluster and ClusterManager to maggma [\#66](https://github.com/materialsproject/maggma/pull/66) ([wuxiaohua1011](https://github.com/wuxiaohua1011))

## [v0.18.0](https://github.com/materialsproject/maggma/tree/v0.18.0) (2020-03-23)

[Full Changelog](https://github.com/materialsproject/maggma/compare/v0.17.3...v0.18.0)

**Merged pull requests:**

- Amazon S3 store update [\#110](https://github.com/materialsproject/maggma/pull/110) ([munrojm](https://github.com/munrojm))

## [v0.17.3](https://github.com/materialsproject/maggma/tree/v0.17.3) (2020-03-18)

[Full Changelog](https://github.com/materialsproject/maggma/compare/v0.17.2...v0.17.3)

## [v0.17.2](https://github.com/materialsproject/maggma/tree/v0.17.2) (2020-03-13)

[Full Changelog](https://github.com/materialsproject/maggma/compare/v0.17.1...v0.17.2)

## [v0.17.1](https://github.com/materialsproject/maggma/tree/v0.17.1) (2020-03-12)

[Full Changelog](https://github.com/materialsproject/maggma/compare/v0.16.1...v0.17.1)

**Merged pull requests:**

- Various Bug Fixes [\#109](https://github.com/materialsproject/maggma/pull/109) ([shyamd](https://github.com/shyamd))
- Addition of Projection Builder [\#99](https://github.com/materialsproject/maggma/pull/99) ([acrutt](https://github.com/acrutt))
- Fix issues with last\_updated in MapBuilder [\#98](https://github.com/materialsproject/maggma/pull/98) ([shyamd](https://github.com/shyamd))
- autonotebook for tqdm [\#97](https://github.com/materialsproject/maggma/pull/97) ([shyamd](https://github.com/shyamd))

## [v0.16.1](https://github.com/materialsproject/maggma/tree/v0.16.1) (2020-01-28)

[Full Changelog](https://github.com/materialsproject/maggma/compare/v0.16.0...v0.16.1)

## [v0.16.0](https://github.com/materialsproject/maggma/tree/v0.16.0) (2020-01-28)

[Full Changelog](https://github.com/materialsproject/maggma/compare/v0.15.0...v0.16.0)

**Closed issues:**

- Onotology generation from builder [\#59](https://github.com/materialsproject/maggma/issues/59)

**Merged pull requests:**

- Add MongoURIStore [\#93](https://github.com/materialsproject/maggma/pull/93) ([shyamd](https://github.com/shyamd))
- Update distinct to be more like mongo distinct [\#92](https://github.com/materialsproject/maggma/pull/92) ([shyamd](https://github.com/shyamd))
- Add count to maggma store [\#86](https://github.com/materialsproject/maggma/pull/86) ([shyamd](https://github.com/shyamd))

## [v0.15.0](https://github.com/materialsproject/maggma/tree/v0.15.0) (2020-01-23)

[Full Changelog](https://github.com/materialsproject/maggma/compare/v0.14.1...v0.15.0)

**Closed issues:**

- Builder Reporting [\#78](https://github.com/materialsproject/maggma/issues/78)
- ZeroMQ based multi-node processing [\#76](https://github.com/materialsproject/maggma/issues/76)
- Add time limits to process\_item? \(Possibly just in MapBuilder?\) [\#45](https://github.com/materialsproject/maggma/issues/45)

**Merged pull requests:**

- \[WIP\] Builder Reporting [\#80](https://github.com/materialsproject/maggma/pull/80) ([shyamd](https://github.com/shyamd))
- Updated GroupBuilder [\#79](https://github.com/materialsproject/maggma/pull/79) ([shyamd](https://github.com/shyamd))
- New Distributed Processor [\#77](https://github.com/materialsproject/maggma/pull/77) ([shyamd](https://github.com/shyamd))

## [v0.14.1](https://github.com/materialsproject/maggma/tree/v0.14.1) (2020-01-10)

[Full Changelog](https://github.com/materialsproject/maggma/compare/v0.14.0...v0.14.1)

## [v0.14.0](https://github.com/materialsproject/maggma/tree/v0.14.0) (2020-01-10)

[Full Changelog](https://github.com/materialsproject/maggma/compare/v0.13.0...v0.14.0)

**Closed issues:**

- Preserve last\_updated for MapBuilder [\#58](https://github.com/materialsproject/maggma/issues/58)
- Move away from mpi4py [\#51](https://github.com/materialsproject/maggma/issues/51)
- Run serial processor directly from builder [\#48](https://github.com/materialsproject/maggma/issues/48)
- Update while processing [\#42](https://github.com/materialsproject/maggma/issues/42)
- Running JSONStore.connect\(\) multiple times leads to undefined behavior [\#40](https://github.com/materialsproject/maggma/issues/40)
- get\_criteria directly invokes mongo commands [\#38](https://github.com/materialsproject/maggma/issues/38)
- Cursor timeouts common [\#35](https://github.com/materialsproject/maggma/issues/35)
- Possible solution to "stalled" Runner.run ? [\#29](https://github.com/materialsproject/maggma/issues/29)

**Merged pull requests:**

- Release Workflow for Github [\#75](https://github.com/materialsproject/maggma/pull/75) ([shyamd](https://github.com/shyamd))
- Documentation [\#74](https://github.com/materialsproject/maggma/pull/74) ([shyamd](https://github.com/shyamd))
- Reorg code [\#69](https://github.com/materialsproject/maggma/pull/69) ([shyamd](https://github.com/shyamd))
- Updates for new monitoring services [\#67](https://github.com/materialsproject/maggma/pull/67) ([shyamd](https://github.com/shyamd))
- fix GridFSStore [\#64](https://github.com/materialsproject/maggma/pull/64) ([gpetretto](https://github.com/gpetretto))
- Massive refactoring to get ready for v1.0 [\#62](https://github.com/materialsproject/maggma/pull/62) ([shyamd](https://github.com/shyamd))
- Bug Fixes [\#61](https://github.com/materialsproject/maggma/pull/61) ([shyamd](https://github.com/shyamd))
- GridFSStore bug fix [\#60](https://github.com/materialsproject/maggma/pull/60) ([munrojm](https://github.com/munrojm))
- Fix Store serialization with @version [\#57](https://github.com/materialsproject/maggma/pull/57) ([mkhorton](https://github.com/mkhorton))
- Update builder to work with new monty [\#56](https://github.com/materialsproject/maggma/pull/56) ([mkhorton](https://github.com/mkhorton))

## [v0.13.0](https://github.com/materialsproject/maggma/tree/v0.13.0) (2019-03-29)

[Full Changelog](https://github.com/materialsproject/maggma/compare/v0.12.0...v0.13.0)

**Merged pull requests:**

- Add timeout to MapBuilder, store process time [\#54](https://github.com/materialsproject/maggma/pull/54) ([mkhorton](https://github.com/mkhorton))
- Can update pyyaml req? [\#50](https://github.com/materialsproject/maggma/pull/50) ([dwinston](https://github.com/dwinston))
- Concat store [\#47](https://github.com/materialsproject/maggma/pull/47) ([shyamd](https://github.com/shyamd))

## [v0.12.0](https://github.com/materialsproject/maggma/tree/v0.12.0) (2018-11-19)

[Full Changelog](https://github.com/materialsproject/maggma/compare/v0.11.0...v0.12.0)

## [v0.11.0](https://github.com/materialsproject/maggma/tree/v0.11.0) (2018-11-01)

[Full Changelog](https://github.com/materialsproject/maggma/compare/v0.9.0...v0.11.0)

**Merged pull requests:**

- Better printing of validation erorrs [\#46](https://github.com/materialsproject/maggma/pull/46) ([mkhorton](https://github.com/mkhorton))
- Updates to JointStore and MapBuilder [\#44](https://github.com/materialsproject/maggma/pull/44) ([shyamd](https://github.com/shyamd))
- \[WIP\] Improve/refactor examples and move inside maggma namespace [\#30](https://github.com/materialsproject/maggma/pull/30) ([dwinston](https://github.com/dwinston))

## [v0.9.0](https://github.com/materialsproject/maggma/tree/v0.9.0) (2018-10-01)

[Full Changelog](https://github.com/materialsproject/maggma/compare/v0.8.0...v0.9.0)

**Closed issues:**

- Non-obvious error message when trying to query a Store that hasn't been connected [\#41](https://github.com/materialsproject/maggma/issues/41)
- Criteria/properties order of MongoStore.query [\#37](https://github.com/materialsproject/maggma/issues/37)
- tqdm in Jupyter [\#33](https://github.com/materialsproject/maggma/issues/33)
- query args order [\#31](https://github.com/materialsproject/maggma/issues/31)

**Merged pull requests:**

- Simplification of Validator class + tests [\#39](https://github.com/materialsproject/maggma/pull/39) ([mkhorton](https://github.com/mkhorton))
- Fix for Jupyter detection for tqdm [\#36](https://github.com/materialsproject/maggma/pull/36) ([mkhorton](https://github.com/mkhorton))
- Add tqdm widget inside Jupyter [\#34](https://github.com/materialsproject/maggma/pull/34) ([mkhorton](https://github.com/mkhorton))
- Change update\_targets log level from debug to exception [\#32](https://github.com/materialsproject/maggma/pull/32) ([mkhorton](https://github.com/mkhorton))
- Jointstore [\#23](https://github.com/materialsproject/maggma/pull/23) ([montoyjh](https://github.com/montoyjh))

## [v0.8.0](https://github.com/materialsproject/maggma/tree/v0.8.0) (2018-08-22)

[Full Changelog](https://github.com/materialsproject/maggma/compare/v0.6.5...v0.8.0)

**Merged pull requests:**

- Fix mrun with default num\_workers. Add test. [\#28](https://github.com/materialsproject/maggma/pull/28) ([dwinston](https://github.com/dwinston))

## [v0.6.5](https://github.com/materialsproject/maggma/tree/v0.6.5) (2018-06-07)

[Full Changelog](https://github.com/materialsproject/maggma/compare/v0.6.4...v0.6.5)

## [v0.6.4](https://github.com/materialsproject/maggma/tree/v0.6.4) (2018-06-07)

[Full Changelog](https://github.com/materialsproject/maggma/compare/v0.6.3...v0.6.4)

## [v0.6.3](https://github.com/materialsproject/maggma/tree/v0.6.3) (2018-06-07)

[Full Changelog](https://github.com/materialsproject/maggma/compare/v0.6.2...v0.6.3)

**Merged pull requests:**

- Add MongograntStore [\#27](https://github.com/materialsproject/maggma/pull/27) ([dwinston](https://github.com/dwinston))

## [v0.6.2](https://github.com/materialsproject/maggma/tree/v0.6.2) (2018-06-01)

[Full Changelog](https://github.com/materialsproject/maggma/compare/v0.6.1...v0.6.2)

## [v0.6.1](https://github.com/materialsproject/maggma/tree/v0.6.1) (2018-06-01)

[Full Changelog](https://github.com/materialsproject/maggma/compare/v0.6.0...v0.6.1)

**Merged pull requests:**

- Help user if e.g. target store built without lu\_field [\#26](https://github.com/materialsproject/maggma/pull/26) ([dwinston](https://github.com/dwinston))

## [v0.6.0](https://github.com/materialsproject/maggma/tree/v0.6.0) (2018-05-01)

[Full Changelog](https://github.com/materialsproject/maggma/compare/v0.5.0...v0.6.0)

**Implemented enhancements:**

- Progress Bar [\#21](https://github.com/materialsproject/maggma/issues/21)
- Query Engine equivalent [\#9](https://github.com/materialsproject/maggma/issues/9)

**Merged pull requests:**

- Progress Bars for Multiprocess Runner [\#24](https://github.com/materialsproject/maggma/pull/24) ([shyamd](https://github.com/shyamd))
- GridFS Store update: use metadata field, update removes old file\(s\) [\#20](https://github.com/materialsproject/maggma/pull/20) ([dwinston](https://github.com/dwinston))

## [v0.5.0](https://github.com/materialsproject/maggma/tree/v0.5.0) (2018-03-31)

[Full Changelog](https://github.com/materialsproject/maggma/compare/0.4.0...v0.5.0)

**Closed issues:**

- Need from pymongo collection [\#18](https://github.com/materialsproject/maggma/issues/18)

**Merged pull requests:**

- Useability updates [\#19](https://github.com/materialsproject/maggma/pull/19) ([shyamd](https://github.com/shyamd))

## [0.4.0](https://github.com/materialsproject/maggma/tree/0.4.0) (2018-02-28)

[Full Changelog](https://github.com/materialsproject/maggma/compare/0.3.0...0.4.0)

**Merged pull requests:**

- New Multiprocessor and MPI Processor [\#17](https://github.com/materialsproject/maggma/pull/17) ([shyamd](https://github.com/shyamd))
- groupby change for memory/jsonstore [\#16](https://github.com/materialsproject/maggma/pull/16) ([montoyjh](https://github.com/montoyjh))
- Rename Schema to Validator [\#15](https://github.com/materialsproject/maggma/pull/15) ([mkhorton](https://github.com/mkhorton))

## [0.3.0](https://github.com/materialsproject/maggma/tree/0.3.0) (2018-02-01)

[Full Changelog](https://github.com/materialsproject/maggma/compare/v0.2.0...0.3.0)

**Implemented enhancements:**

- Vault enabled Store [\#8](https://github.com/materialsproject/maggma/issues/8)

**Merged pull requests:**

- PR for generic Schema class [\#14](https://github.com/materialsproject/maggma/pull/14) ([mkhorton](https://github.com/mkhorton))
- Issue 8 vault store [\#13](https://github.com/materialsproject/maggma/pull/13) ([shreddd](https://github.com/shreddd))
- adds grouping function and test to make aggregation-based builds [\#12](https://github.com/materialsproject/maggma/pull/12) ([montoyjh](https://github.com/montoyjh))

## [v0.2.0](https://github.com/materialsproject/maggma/tree/v0.2.0) (2018-01-01)

[Full Changelog](https://github.com/materialsproject/maggma/compare/v0.1.0...v0.2.0)

**Closed issues:**

- LU translation functions don't serialize [\#11](https://github.com/materialsproject/maggma/issues/11)

**Merged pull requests:**

- Mongolike mixin [\#10](https://github.com/materialsproject/maggma/pull/10) ([montoyjh](https://github.com/montoyjh))

## [v0.1.0](https://github.com/materialsproject/maggma/tree/v0.1.0) (2017-11-08)

[Full Changelog](https://github.com/materialsproject/maggma/compare/78ef2e8eacc051207350dc6abe886a403982aef8...v0.1.0)

**Closed issues:**

- ditch python 2 and support only 3? [\#3](https://github.com/materialsproject/maggma/issues/3)
- Seeking clarifications [\#1](https://github.com/materialsproject/maggma/issues/1)

**Merged pull requests:**

- Do not wait until all items are processed to update targets [\#7](https://github.com/materialsproject/maggma/pull/7) ([dwinston](https://github.com/dwinston))
- Run builder with either MPI or multiprocessing [\#6](https://github.com/materialsproject/maggma/pull/6) ([matk86](https://github.com/matk86))
- add lava code and tool execution script [\#5](https://github.com/materialsproject/maggma/pull/5) ([gilbertozp](https://github.com/gilbertozp))
- Add eclipse project files to .gitignore [\#2](https://github.com/materialsproject/maggma/pull/2) ([gilbertozp](https://github.com/gilbertozp))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
