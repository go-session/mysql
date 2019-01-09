package mysql

import (
	"context"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	. "github.com/smartystreets/goconvey/convey"
)

const (
	dsn     = "root:@tcp(127.0.0.1:3306)/myapp_test?charset=utf8"
	expired = 5
)

func newSid() string {
	return "test_mysql_store_" + time.Now().String()
}

func TestStore(t *testing.T) {
	mstore := NewDefaultStore(NewConfig(dsn))
	defer mstore.Close()

	Convey("Test mysql store operation", t, func() {
		sid := newSid()
		defer mstore.Delete(context.Background(), sid)

		store, err := mstore.Create(context.Background(), sid, expired)
		So(err, ShouldBeNil)
		foo, ok := store.Get("foo")
		So(ok, ShouldBeFalse)
		So(foo, ShouldBeNil)

		store.Set("foo", "bar")
		store.Set("foo2", "bar2")
		err = store.Save()
		So(err, ShouldBeNil)

		foo, ok = store.Get("foo")
		So(ok, ShouldBeTrue)
		So(foo, ShouldEqual, "bar")

		foo = store.Delete("foo")
		So(foo, ShouldEqual, "bar")

		foo, ok = store.Get("foo")
		So(ok, ShouldBeFalse)
		So(foo, ShouldBeNil)

		foo2, ok := store.Get("foo2")
		So(ok, ShouldBeTrue)
		So(foo2, ShouldEqual, "bar2")

		err = store.Flush()
		So(err, ShouldBeNil)

		foo2, ok = store.Get("foo2")
		So(ok, ShouldBeFalse)
		So(foo2, ShouldBeNil)
	})
}

func TestManagerStore(t *testing.T) {
	mstore := NewDefaultStore(NewConfig(dsn))
	defer mstore.Close()

	Convey("Test mysql-based store management operations", t, func() {
		sid := newSid()

		store, err := mstore.Create(context.Background(), sid, expired)
		So(store, ShouldNotBeNil)
		So(err, ShouldBeNil)

		store.Set("foo", "bar")
		err = store.Save()
		So(err, ShouldBeNil)

		store, err = mstore.Update(context.Background(), sid, expired)
		So(store, ShouldNotBeNil)
		So(err, ShouldBeNil)

		foo, ok := store.Get("foo")
		So(ok, ShouldBeTrue)
		So(foo, ShouldEqual, "bar")

		newsid := newSid()
		store, err = mstore.Refresh(context.Background(), sid, newsid, expired)
		So(store, ShouldNotBeNil)
		So(err, ShouldBeNil)

		foo, ok = store.Get("foo")
		So(ok, ShouldBeTrue)
		So(foo, ShouldEqual, "bar")

		exists, err := mstore.Check(context.Background(), sid)
		So(exists, ShouldBeFalse)
		So(err, ShouldBeNil)

		err = mstore.Delete(context.Background(), newsid)
		So(err, ShouldBeNil)

		exists, err = mstore.Check(context.Background(), newsid)
		So(exists, ShouldBeFalse)
		So(err, ShouldBeNil)
	})
}
