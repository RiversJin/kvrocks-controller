package common

import (
	"fmt"

	"github.com/apache/kvrocks-controller/util"
)

type ControllerID struct {
	Addr             string `json:"addr"`
	IDC              string `json:"idc"`
	UniqueIdentifier string `json:"-"`
	encoded          string `json:"-"`
}

func (c ControllerID) String() string {
	if c.encoded == "" {
		c.encoded = fmt.Sprintf("%s/%s_%s", c.IDC, c.Addr, c.UniqueIdentifier)
	}
	return c.encoded
}

func ParseControllerID(s string) (ControllerID, error) {
	var id ControllerID
	_, err := fmt.Sscanf(s, "%s/%s_%s", &id.IDC, &id.Addr, &id.UniqueIdentifier)
	if err != nil {
		return ControllerID{}, err
	}

	id.encoded = s
	return id, nil
}

func NewControllerID(addr string, idc string) ControllerID {
	return ControllerID{
		Addr:             addr,
		IDC:              idc,
		UniqueIdentifier: util.RandString(8),
		encoded:          "",
	}
}
