-- 낙관적 락용 버전 컬럼. VersionedStock(@Version)만 이 컬럼을 읽고 올린다.
-- Stock 엔티티는 이 컬럼을 매핑하지 않으므로 INSERT 시 기본값 0이 들어간다.
ALTER TABLE stock ADD COLUMN version BIGINT NOT NULL DEFAULT 0;
