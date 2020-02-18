import scala.math.Ordered;
import spire.random.rng.Serial;

import java.io.Serializable;

/*
* 算子需要网络IO，因此需要加载支持序列化
* */
public class SecondarySortKey implements Ordered<SecondarySortKey>, Serializable {

    // 自定义需要排序的列
    private int _first;
    private int _second;

    public SecondarySortKey(int _other1,int _other2) {
        _first = _other1;
        _second = _other2;
    }

    @Override
    public int compare(SecondarySortKey that) {
        if((this._first-that.get_first()) != 0)
            return this._first-that.get_first();
        else return this._second - that.get_second();
    }

    @Override
    public boolean $less(SecondarySortKey that) {
        if(this._first<that.get_first())
            return true;
        else if(this._first == that.get_first() && this._second<that.get_second())
            return true;
        return false;
    }

    @Override
    public boolean $greater(SecondarySortKey that) {
        if(this._first>that.get_first())
            return true;
        else if(this._first == this.get_first() && this._second>that.get_second())
            return true;
        return false;
    }

    @Override
    public boolean $less$eq(SecondarySortKey that) {
        if(this.$less(that))
            return true;
        else if(this._first == that.get_first()&& this._second== that.get_second())
            return true;
        return false;
    }

    @Override
    public boolean $greater$eq(SecondarySortKey that) {
        if(this.$greater(that))
            return true;
        else if(this._first == that.get_first()&& this._second== that.get_second())
            return true;
        return false;
    }

    @Override
    public int compareTo(SecondarySortKey that) {
        if((this._first-that.get_first()) != 0)
            return this._first-that.get_first();
        else return this._second - that.get_second();
    }

    public int get_first(){
        return this._first;
    }

    public int get_second(){
        return this._second;
    }
}
