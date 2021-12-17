package base

import (
	"encoding/json"
	"path"

	"github.com/gogo/protobuf/proto"
	"github.com/tikv/pd/pkg/errs"
)

func (s *Storage) loadProto(key string, msg proto.Message) (bool, error) {
	value, err := s.Load(key)
	if err != nil {
		return false, err
	}
	if value == "" {
		return false, nil
	}
	err = proto.Unmarshal([]byte(value), msg)
	if err != nil {
		return false, errs.ErrProtoUnmarshal.Wrap(err).GenWithStackByCause()
	}
	return true, nil
}

func (s *Storage) saveProto(key string, msg proto.Message) error {
	value, err := proto.Marshal(msg)
	if err != nil {
		return errs.ErrProtoMarshal.Wrap(err).GenWithStackByCause()
	}
	return s.Save(key, string(value))
}

func (s *Storage) saveJSON(prefix, key string, data interface{}) error {
	value, err := json.Marshal(data)
	if err != nil {
		return errs.ErrJSONMarshal.Wrap(err).GenWithStackByArgs()
	}
	return s.Save(path.Join(prefix, key), string(value))
}
