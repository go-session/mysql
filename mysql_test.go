package mysql

import (
	"context"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	. "github.com/smartystreets/goconvey/convey"
)

const (
	dsn = "root:123456@tcp(127.0.0.1:3306)/myapp_test?charset=utf8"
)

func TestStore(t *testing.T) {
	mstore := NewStore(NewConfig(dsn), "", 1)
	defer mstore.Close()

	Convey("Test mysql store operation", t, func() {
		store, err := mstore.Create(context.Background(), "test_mysql_store", 10)
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
	mstore := NewStore(NewConfig(dsn), "", 1)
	defer mstore.Close()

	Convey("Test mysql-based store management operations", t, func() {
		sid := "test_manager_store"
		store, err := mstore.Create(context.Background(), sid, 10)
		So(store, ShouldNotBeNil)
		So(err, ShouldBeNil)

		store.Set("foo", "bar")
		err = store.Save()
		So(err, ShouldBeNil)

		store, err = mstore.Update(context.Background(), sid, 10)
		So(store, ShouldNotBeNil)
		So(err, ShouldBeNil)

		foo, ok := store.Get("foo")
		So(ok, ShouldBeTrue)
		So(foo, ShouldEqual, "bar")

		newsid := "test_manager_store2"
		store, err = mstore.Refresh(context.Background(), sid, newsid, 10)
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
