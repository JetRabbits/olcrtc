package crypto

import "testing"

// pskFixture is a fixed 32-byte PSK used only by tests in this file.
var pskFixture = []byte{
	0x9b, 0x2f, 0x41, 0xd7, 0x0c, 0x88, 0x5a, 0xe3,
	0x61, 0x14, 0xfb, 0x22, 0x99, 0x70, 0x3d, 0x46,
	0xae, 0x58, 0x07, 0xc4, 0x12, 0xed, 0x6b, 0x80,
	0x35, 0x9c, 0x4e, 0xa1, 0x77, 0x02, 0xbb, 0xd9,
}

// TestInterleavedConnectionsUseIndependentReplayWindows pins the desired
// contract after the historical bulk-transfer bug: a control record sealed
// before a data session races hundreds of records ahead must still open because
// each connection has its own nonce prefix and therefore its own replay window.
func TestInterleavedConnectionsUseIndependentReplayWindows(t *testing.T) {
	sender, err := NewKeySet(pskFixture, Client)
	if err != nil {
		t.Fatalf("NewKeySet(sender) error = %v", err)
	}
	receiver, err := NewKeySet(pskFixture, Server)
	if err != nil {
		t.Fatalf("NewKeySet(receiver) error = %v", err)
	}
	controlSession, err := sender.Session()
	if err != nil {
		t.Fatalf("Session(control) error = %v", err)
	}
	dataSession, err := sender.Session()
	if err != nil {
		t.Fatalf("Session(data) error = %v", err)
	}

	control, err := controlSession.Seal([]byte("control: ping"), nil)
	if err != nil {
		t.Fatalf("Seal(control) error = %v", err)
	}

	const bulkRecords = replayWindowSize * 8
	body := make([]byte, 8)
	sealed := make([][]byte, 0, bulkRecords)
	for i := range bulkRecords {
		body[0] = byte(i)
		rec, err := dataSession.Seal(body[:], nil)
		if err != nil {
			t.Fatalf("Seal(data %d) error = %v", i, err)
		}
		sealed = append(sealed, rec)
	}

	for i, rec := range sealed {
		if _, err := receiver.Open(rec, nil); err != nil {
			t.Fatalf("Open(data %d) error = %v", i, err)
		}
	}
	if got, err := receiver.Open(control, nil); err != nil || string(got) != "control: ping" {
		t.Fatalf("Open(control) = %q, %v; want control payload and nil error", got, err)
	}
}
