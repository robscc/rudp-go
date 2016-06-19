package rudp

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"errors"
)

const (
	DefaultPackageSize = 512
	ErrCorrupt         = errors.New("corrupt")
)

const (
	TypeIgnore = iota
	TypeCorrupt
	TypeRequest
	TypeMissing
	TypeNormal
)

type Message struct {
	Size int
	Cap  int
	ID   int
	Tick int
	Buf  []byte
}

type RUDPPackage struct {
	Size int
	Buf  []byte
}

//Array
type PackageIndex struct {
	Cap int
	N   int
	A   []int
}

type RUDP struct {
	SendQueue   *list.List
	RecvQueue   *list.List
	SendHistory *list.List

	SendPackage *list.List

	FreeList  *list.List
	SendAgain PackageIndex //Array

	Corrupt        int
	CurrentTick    int
	LastSendTick   int
	LastExpireTick int
	SendId         int
	RecvIdMin      int
	RecvIdMax      int
	SendDelay      int
	Expired        int
}

func RUDPNew(sendDlay int, ExpireTime int) *RUDP {
	u := new(RUDP)
	u.SendQueue = list.New()
	u.RecvQueue = list.New()
	u.SendHistory = list.New()

	u.SendPackage = list.New()
	u.SendDelay = sendDlay
	u.Expired = ExpireTime
	return u
}

func ClearOutPacakge(u *RUDP) {
	for e := u.SendPackage.Front(); e != nil; e = e.Next() {
		u.SendPackage.Remove(e)
	}
}

func ClearMessageQueue(l *list.List) {
	for e := l.Front(); e != nil; e = e.Next() {
		l.Remove(e)
	}
}

func RUDPDelete(u *RUDP) {
	ClearMessageQueue(u.SendQueue)
	ClearMessageQueue(u.RecvQueue)
	ClearMessageQueue(u.SendHistory)
	ClearMessageQueue(u.FreeList)
	ClearOutPacakge(u)
}

func NewMessage(u *RUDP, buf []byte, size int) *Message {
	ele := make(Message)
	copy(ele.Buf, buf)
	ele.Tick = 0
	ele.ID = 0
	ele.Size = size
	ele.Cap = DefaultPackageSize

	u.FreeList.PushFront(ele)
	return ele
}

func RemoveMessage(u *RUDP, m *Message) {
	if m == nil {
		return
	}
	for e := u.FreeList.Front(); e != nil; e = e.Next() {
		msg, ok := e.Value.(*Message)
		if ok && msg != nil {
			if msg == m {
				u.FreeList.Remove(e)
			}
		}
	}
}

func QueuePush(q *list.List, m *Message) {
	if q != nil && m != nil {
		q.PushFront(m)
	}
}

func QueuePop(q *list.List, id int) *Message {
	for e := q.Front(); e != nil; e = e.Next() {
		msg, ok := e.Value.(*Message)
		if ok && msg != nil {
			if msg.ID == id {
				q.Remove(e)
				return msg
			}
		}
	}
	return nil
}

func RUDPSend(u *RUDP, buf []byte, size int) {
	m := NewMessage(u, buf, size)
	u.SendId++
	m.ID = u.SendId
	m.Tick = u.CurrentTick
	QueuePush(u.SendQueue, m)
}

func RUDPRecv(u *RUDP) (res []byte, err error) {
	if u.Corrupt {
		u.Corrupt = 0
		return res, ErrCorrupt
	}

	m := QueuePop(u.RecvQueue, u.RecvIdMin)
	if m == nil {
		return res, nil
	}
	u.RecvIdMin++
	copy(res, m.Buf)
	RemoveMessage(u, m)
	return
}

func ClearSendExpired(u *RUDP, tick int) {
	for e := u.SendHistory.Back(); e != nil; e = e.Prev() {
		msg, ok := e.Value.(*Message)
		if ok {
			if msg.Tick >= tick {
				u.SendHistory.Remove(e)
			}
		}
	}
}

func GetID(u *RUDP, buf []byte) int {
	id := int(buf[0])*256 + int(buf[1])
	id |= u.RecvIdMax & (^0xffff)
	if id < u.RecvIdMax-0x8000 {
		id += 0x100000
	} else if id > u.RecvIdMax+0x8000 {
		id -= 0x100000
	}
	return id
}

func InsertMessage(u *RUDP, id int, buf []byte, size int) {
	if id < u.RecvIdMin {
		return
	}
	if id > u.RecvIdMax || u.RecvQueue.Front() == nil {
		m := NewMessage(u, buf, size)
		m.ID = id
		QueuePush(u.RecvQueue, m)
		u.RecvIdMax = id
	} else {
		for e := u.RecvQueue.Back(); e != nil; e = e.Prev() {
			msg, ok := e.Value.(*Message)
			if ok {
				if msg.ID > id {
					m := NewMessage(u, buf, size)
					u.RecvQueue.InsertAfter(u, e)
					return
				}
			}
		}
	}
}

func AddMissing(u *RUDP, id int) {
	InsertMessage(u, id, nil, -1)
}

func ExtractPackage(u *RUDP, buf []byte, size int) {
	for size > 0 {
		length := intbuf[0]
		if length > 127 {
			if size <= 1 {
				u.Corrupt = 1
				return
			}
			length = (length*256 + buffer[1]) & 0x7fff
			buf = buf[2:]
			size = size - 2
		} else {
			buf = buf[1:]
			size = size - 1
		}
		switch length {
		case TypeIgnore:
			if u.SendAgain.N == 0 {
				u.SendAgain.A = append(u.SendAgain.A, u.RecvIdMin)
			}
		case TypeCorrupt:
			u.Corrupt = 1
			return
		case TypeRequest:
			fallthrough
		case TypeMissing:
			if size < 2 {
				u.Corrupt = 1
				return
			}
			if length == TypeRequest {
				u.SendAgain.A = append(u.SendAgain.A, GetID(u, buf))
			} else {
				AddMissing(u, GetID(u, buf))
			}
			buf = buf[2:]
			size = size - 2
		default:
			length = length - TypeNormal
			if size < length+2 {
				u.Corrupt = 1
				return
			} else {
				id := GetID(u, buf)
				InsertMessage(u, id, buf[2:], length)
			}
			buf = buf[length+2:]
			size = size - length - 2
		}
	}
}

type TmpBuffer struct {
	Buf      [DefaultPackageSize]byte
	Size     int
	Packages *list.List
}

func NewPacakge(u *RUDP, tmp *TmpBuffer) {
	p := RUDPPackage{}
	p.Size = tmp.Size
	copy(p.Buf, tmp.Buf)
	tmp.Packages.PushBack(p)
	tmp.Size = 0
}

func FillHeader(buf []byte, length int, id int) (offset int, size int) {
	//var size int
	if length < 128 {
		buf[0] = length
		size = 1
		offset = 1
	} else {
		buf[0] = ((lenn & 0x7f00) >> 8) | 0x89
		buf[1] = len & 0xff
		size = 2
		offset = 2
	}
	buf[offset] = (id & 0xff00) >> 8
	buf[offset+1] = (id & 0xff)
	size = size + 2
	offset = offset + 2
	return size
}

func PackRequest(u *RUDP, tmp *TmpBuffer, id int, tag int) {
	size := DefaultPackageSize - tmp.Size
	if size < 3 {
		NewPacakge(u, tmp)
	}
	_, size = FillHeader(tmp.Buf, tag, id)
	tmp.Size += size
}

func PackMessage(u *RUDP, tmp *TmpBuffer, m *Message) {
	size := DefaultPackageSize - tmp.Size
	if m.Size > DefaultPackageSize-4 {
		if tmp.Size > 0 {
			NewPacakge(u, tmp)
		}
		size = 4 + m.Size
		p := &RUDPPackage{}
		FillHeader(p.Buf, m.Size+TypeNormal, m.ID)
		p.Buf = append(p.Buf[0:4], m.Buf...)
		tmp.Packages.PushBack(p)
	}
}

func RequestMissing(u *RUDP, tmp *TmpBuffer) {
	id := u.RecvIdMin
	//m = u.RecvQueue.Front()
	for m := u.RecvQueue.Front(); m != nil; m = m.Next() {
		ele, _ := (*Message)(m)
		if ele.ID > id {
			for i := id; i < ele.ID; i++ {
				PackRequest(u, tmp, i, TypeRequest)
			}
		}
		id = ele.ID + 1
	}
}

func ReplyRequest(u *RUDP, tmp *TmpBuffer) {
	m := u.SendHistory.Front()
	for i := 0; i < u.SendAgain.N; i++ {
		id := u.SendAgain.A[i]
		if id < u.RecvIdMin {
			continue
		}
		for {
			if m != nil {
				his, _ := (*Message)(m)
				if id < his.ID {
					PackRequest(u, tmp, id, TypeMissing)
					break
				} else if id == his.ID {
					PackMessage(u, tmp, his)
					break
				}
			} else {
				PackRequest(u, tmp, id, TypeMissing)
				break
			}
			m = m.Next()
		}
	}
	u.SendAgain.N = 0
}

func SendMessage(u *RUDP, tmp *TmpBuffer) {
	for m := u.SendQueue.Front(); m != nil; m = m.Next() {
		ele, _ := (*Message)(m)
		PackMessage(u, tmp, ele)
		u.SendHistory.PushBack(m)
		u.SendQueue.Remove(m)
	}
}

func GenOutPackage(u *RUDP) *list.List {
	tmp := TmpBuffer{}
	tmp.Size = 0

	RequestMissing(u, &tmp)
	ReplyRequest(u, &tmp)
	SendMessage(u, &tmp)

	if tmp.Packages.Front() == nil {
		if tmp.Size == 0 {
			tmp.Buf[0] = TypeIgnore
			tmp.Size = 1
		}
	}
	NewPacakge(u, &tmp)
	//return package
}

func RUDPUpdate(u *RUDP, buf []byte, size int, tick int) *list.List {
	u.CurrentTick = u.CurrentTick + tick
	ClearOutPacakge(u)
	ExtractPackage(u, buf, size)

	if u.CurrentTick > u.LastExpireTick+u.Expired {
		ClearSendExpired(u, u.LastExpireTick)
		u.LastExpireTick = u.CurrentTick
	}
	if u.CurrentTick >= u.LastSendTick+u.SendDelay {
		u.SendPackage = GenOutPackage(u)
		u.LastSendTick = u.CurrentTick
		return u.SendPackage
	} else {
		return nil
	}
}
