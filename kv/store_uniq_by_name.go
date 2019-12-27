package kv

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kit/tracing"
)

type Entity struct {
	ID    influxdb.ID
	Name  string
	OrgID influxdb.ID
	Body  interface{}
}

type EncodeEntFn func(ent Entity) ([]byte, string, error)

func EncIDKey(ent Entity) ([]byte, string, error) {
	id, err := ent.ID.Encode()
	return id, "ID", err
}

func EncOrgNameKeyFn(caseSensitive bool) func(ent Entity) ([]byte, string, error) {
	return func(ent Entity) ([]byte, string, error) {
		key, err := indexByOrgNameKey(ent.OrgID, ent.Name, caseSensitive)
		return key, "organization ID and name", err
	}
}

func EncBodyJSON(ent Entity) ([]byte, string, error) {
	v, err := json.Marshal(ent.Body)
	return v, "entity body", err
}

type DecodeBucketEntFn func(key, val []byte) (keyRepeat []byte, decodedVal interface{}, err error)

func DecIndexEntFn(key, val []byte) ([]byte, interface{}, error) {
	var i influxdb.ID
	return key, i, i.Decode(val)
}

type DecodedValToEntFn func(k []byte, v interface{}) (Entity, error)

func DecodeOrgNameKey(k []byte) (influxdb.ID, string, error) {
	var orgID influxdb.ID
	if err := orgID.Decode(k[:influxdb.IDLength]); err != nil {
		return 0, "", err
	}
	return orgID, string(k[influxdb.IDLength:]), nil
}

func newIndexStore(kv Store, resource string, bktName []byte, caseSensitive bool) *store {
	var decValToEntFn DecodedValToEntFn = func(k []byte, v interface{}) (Entity, error) {
		orgID, name, err := DecodeOrgNameKey(k)
		if err != nil {
			return Entity{}, err
		}

		idBytes, ok := v.([]byte)
		if err := errUnexpectedDecodeVal(ok); err != nil {
			return Entity{}, err
		}

		ent := Entity{OrgID: orgID, Name: name}
		return ent, ent.ID.Decode(idBytes)
	}

	return newStore(kv, resource, bktName, EncOrgNameKeyFn(caseSensitive), EncIDKey, DecIndexEntFn, decValToEntFn)
}

type store struct {
	kv       Store
	resource string
	bktName  []byte

	encodeEntKeyFn    EncodeEntFn
	encodeEntBodyFn   EncodeEntFn
	decodeBucketEntFn DecodeBucketEntFn
	decodeToEntFn     DecodedValToEntFn
}

func newStore(kv Store, resource string, bktName []byte, encKeyFn, encBodyFn EncodeEntFn, decFn DecodeBucketEntFn, decToEntFn DecodedValToEntFn) *store {
	return &store{
		kv:                kv,
		resource:          resource,
		bktName:           bktName,
		encodeEntKeyFn:    encKeyFn,
		encodeEntBodyFn:   encBodyFn,
		decodeBucketEntFn: decFn,
		decodeToEntFn:     decToEntFn,
	}
}

func (s *store) Init(ctx context.Context, tx Tx) error {
	span, _ := tracing.StartSpanFromContextWithOperationName(ctx, "bucket_"+string(s.bktName))
	defer span.Finish()

	if _, err := s.bucket(ctx, tx); err != nil {
		return &influxdb.Error{
			Code: influxdb.EInternal,
			Msg:  fmt.Sprintf("failed to create bucket: %s", string(s.bktName)),
			Err:  err,
		}
	}
	return nil
}

type DeleteOpts struct {
	DeleteRelationFns []func(k []byte, v interface{}) error
	FilterFn          func(k []byte, v interface{}) bool
}

func (s *store) Delete(ctx context.Context, tx Tx, opts DeleteOpts) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if opts.FilterFn == nil {
		return nil
	}

	var o influxdb.FindOptions
	return s.Find(ctx, tx, o, opts.FilterFn, func(k []byte, v interface{}) error {
		for _, deleteFn := range opts.DeleteRelationFns {
			if err := deleteFn(k, v); err != nil {
				return err
			}
		}
		return s.bucketDelete(ctx, tx, k)
	})
}

func (s *store) DeleteEnt(ctx context.Context, tx Tx, ent Entity) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	encodedID, err := s.encodeEnt(ctx, ent, s.encodeEntKeyFn)
	if err != nil {
		return err
	}
	return s.bucketDelete(ctx, tx, encodedID)
}

func (s *store) Find(ctx context.Context, tx Tx, opt influxdb.FindOptions, filterFn func(k []byte, v interface{}) bool, captureFn func(k []byte, v interface{}) error) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	cur, err := s.bucketCursor(ctx, tx)
	if err != nil {
		return err
	}

	iter := &Iter{
		cursor:     cur,
		descending: opt.Descending,
		offset:     opt.Offset,
		limit:      opt.Limit,
		decodeFn:   s.decodeBucketEntFn,
		filterFn:   filterFn,
	}

	for k, v := iter.Next(ctx); k != nil; k, v = iter.Next(ctx) {
		if err := captureFn(k, v); err != nil {
			return err
		}
	}
	return nil
}

func (s *store) FindEnt(ctx context.Context, tx Tx, ent Entity) (interface{}, error) {
	encodedID, err := s.encodeEnt(ctx, ent, s.encodeEntKeyFn)
	if err != nil {
		// TODO: fix this error up
		return nil, err
	}

	body, err := s.bucketGet(ctx, tx, encodedID)
	if err != nil {
		return nil, err
	}

	return s.decodeEnt(ctx, body)
}

func (s *store) Put(ctx context.Context, tx Tx, ent Entity) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	encodedID, err := s.encodeEnt(ctx, ent, s.encodeEntKeyFn)
	if err != nil {
		return err
	}

	body, err := s.encodeEnt(ctx, ent, s.encodeEntBodyFn)
	if err != nil {
		return err
	}

	return s.bucketPut(ctx, tx, encodedID, body)
}

func (s *store) bucket(ctx context.Context, tx Tx) (Bucket, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	bkt, err := tx.Bucket(s.bktName)
	if err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInternal,
			Msg:  fmt.Sprintf("unexpected error retrieving bucket %q; Err %v", string(s.bktName), err),
			Err:  err,
		}
	}
	return bkt, nil
}

func (s *store) bucketCursor(ctx context.Context, tx Tx) (Cursor, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	b, err := s.bucket(ctx, tx)
	if err != nil {
		return nil, err
	}

	cur, err := b.Cursor()
	if err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInternal,
			Msg:  fmt.Sprintf("failed to retrieve cursor"),
			Err:  err,
		}
	}
	return cur, nil
}

func (s *store) bucketDelete(ctx context.Context, tx Tx, key []byte) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	b, err := s.bucket(ctx, tx)
	if err != nil {
		return err
	}

	err = b.Delete(key)
	if err == nil {
		return nil
	}

	iErr := &influxdb.Error{
		Code: influxdb.EInternal,
		Err:  err,
	}
	if IsNotFound(err) {
		iErr.Code = influxdb.ENotFound
		iErr.Msg = fmt.Sprintf("%s does exist for key: %q", s.resource, string(key))
	}
	return iErr
}

func (s *store) bucketGet(ctx context.Context, tx Tx, key []byte) ([]byte, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	b, err := s.bucket(ctx, tx)
	if err != nil {
		return nil, err
	}

	body, err := b.Get(key)
	if IsNotFound(err) {
		return nil, &influxdb.Error{
			Code: influxdb.ENotFound,
			Msg:  fmt.Sprintf("%s not found for key %q", s.resource, string(key)),
		}
	}
	if err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInternal,
			Err:  err,
		}
	}

	return body, nil
}

func (s *store) bucketPut(ctx context.Context, tx Tx, key, body []byte) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	b, err := s.bucket(ctx, tx)
	if err != nil {
		return err
	}

	if err := b.Put(key, body); err != nil {
		return &influxdb.Error{
			Code: influxdb.EInternal,
			Err:  err,
		}
	}
	return nil
}

func (s *store) decodeEnt(ctx context.Context, body []byte) (interface{}, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	_, v, err := s.decodeBucketEntFn([]byte{}, body) // ignore key here
	if err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInternal,
			Msg:  fmt.Sprintf("failed to decode %s body", s.resource),
			Err:  err,
		}
	}
	return v, nil
}

func (s *store) encodeEnt(ctx context.Context, ent Entity, fn EncodeEntFn) ([]byte, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	encoded, field, err := fn(ent)
	if err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  fmt.Sprintf("provided %s %s is an invalid format", s.resource, field),
			Err:  err,
		}
	}
	return encoded, nil
}

type uniqByNameStore struct {
	resource   string
	entStore   *store
	indexStore *store

	decodeEntOrgNameFn func(i interface{}) (orgID influxdb.ID, name string, err error)
}

func (s *uniqByNameStore) Init(ctx context.Context, tx Tx) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	initFns := []func(context.Context, Tx) error{
		s.entStore.Init,
		s.indexStore.Init,
	}
	for _, fn := range initFns {
		if err := fn(ctx, tx); err != nil {
			return err
		}
	}
	return nil
}

func (s *uniqByNameStore) Delete(ctx context.Context, tx Tx, opts DeleteOpts) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	deleteRelationFn := func(k []byte, v interface{}) error {
		ent, err := s.entStore.decodeToEntFn(k, v)
		if err != nil {
			return err
		}
		return s.indexStore.DeleteEnt(ctx, tx, ent)
	}
	opts.DeleteRelationFns = append(opts.DeleteRelationFns, deleteRelationFn)
	return s.entStore.Delete(ctx, tx, opts)
}

func (s *uniqByNameStore) DeleteEnt(ctx context.Context, tx Tx, id influxdb.ID) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	existing, err := s.FindByID(ctx, tx, id)
	if err != nil {
		return err
	}

	orgID, name, err := s.decodeEntOrgName(existing)
	if err != nil {
		return err
	}

	if err := s.entStore.DeleteEnt(ctx, tx, Entity{ID: id}); err != nil {
		return err
	}

	return s.indexStore.DeleteEnt(ctx, tx, Entity{OrgID: orgID, Name: name})
}

func (s *uniqByNameStore) Find(ctx context.Context, tx Tx, opt influxdb.FindOptions, filterFn func(k []byte, v interface{}) bool, captureFn func(k []byte, v interface{}) error) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	return s.entStore.Find(ctx, tx, opt, filterFn, captureFn)
}

func (s *uniqByNameStore) FindByID(ctx context.Context, tx Tx, id influxdb.ID) (interface{}, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	return s.entStore.FindEnt(ctx, tx, Entity{ID: id})
}

func (s *uniqByNameStore) FindByName(ctx context.Context, tx Tx, ent Entity) (interface{}, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	idxEncodedID, err := s.indexStore.FindEnt(ctx, tx, ent)
	if err != nil {
		return nil, err
	}

	return s.entStore.FindEnt(ctx, tx, Entity{
		ID: idxEncodedID.(influxdb.ID),
	})
}

func (s *uniqByNameStore) Put(ctx context.Context, tx Tx, ent Entity) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if err := s.indexStore.Put(ctx, tx, ent); err != nil {
		return err
	}

	return s.entStore.Put(ctx, tx, ent)
}

func (s *uniqByNameStore) decodeEntOrgName(v interface{}) (influxdb.ID, string, error) {
	orgID, name, err := s.decodeEntOrgNameFn(v)
	if err != nil {
		return 0, "", &influxdb.Error{
			Code: influxdb.EInternal,
			Msg:  "failed to decoded body",
			Err:  err,
		}
	}
	return orgID, name, nil
}

type Iter struct {
	cursor Cursor

	counter    int
	descending bool
	limit      int
	offset     int

	decodeFn func(key, val []byte) (k []byte, decodedVal interface{}, err error)
	filterFn func(key []byte, decodedVal interface{}) bool
}

func (i *Iter) Next(ctx context.Context) (key []byte, val interface{}) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()
	if i.limit > 0 && i.counter >= i.limit+i.offset {
		return nil, nil
	}

	var (
		k, vRaw []byte
		nextFn  func() ([]byte, []byte)
	)
	switch {
	case i.counter == 0 && i.descending:
		k, vRaw = i.cursor.Last()
		nextFn = i.cursor.Prev
	case i.counter == 0:
		k, vRaw = i.cursor.First()
		nextFn = i.cursor.Next
	case i.descending:
		k, vRaw = i.cursor.Prev()
		nextFn = i.cursor.Prev
	default:
		k, vRaw = i.cursor.Next()
		nextFn = i.cursor.Next
	}

	k, decodedVal, err := i.decodeFn(k, vRaw)
	for ; err == nil && len(k) > 0; k, decodedVal, err = i.decodeFn(nextFn()) {
		if i.isNext(k, decodedVal) {
			break
		}
	}
	return k, decodedVal
}

func (i *Iter) isNext(k []byte, v interface{}) bool {
	if len(k) == 0 {
		return true
	}

	if i.filterFn != nil && !i.filterFn(k, v) {
		return false
	}

	// increase counter here since the entity is a valid ent
	// and counts towards the total the user is looking for
	// 	i.e. limit = 5 => 5 valid ents
	//	i.e. offset = 5 => return valid ents after seeing 5 valid ents
	i.counter++

	if i.limit > 0 && i.counter >= i.limit+i.offset {
		return true
	}
	if i.offset > 0 && i.counter <= i.offset {
		return false
	}
	return true
}

func indexByOrgNameKey(orgID influxdb.ID, name string, caseSensitive bool) ([]byte, error) {
	if name == "" {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "name must be provided",
		}
	}

	orgIDEncoded, err := orgID.Encode()
	if err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  fmt.Sprintf("invalid org ID provided: %q", orgID.String()),
			Err:  err,
		}
	}
	k := make([]byte, influxdb.IDLength+len(name))
	copy(k, orgIDEncoded)
	if !caseSensitive {
		name = strings.ToLower(name)
	}
	copy(k[influxdb.IDLength:], name)
	return k, nil
}

func errUnexpectedDecodeVal(ok bool) error {
	if ok {
		return nil
	}
	return errors.New("unexpected value decoded")
}
