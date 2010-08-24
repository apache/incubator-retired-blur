package using;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.nearinfinity.blur.thrift.generated.Hit;

public class HitsTest {

	public static void main(String[] args) {
		
		List<Hit> hits = new ArrayList<Hit>();
		
		hits.add(new Hit("1", 2.0, ""));
		hits.add(new Hit("2", 2.1, ""));
		hits.add(new Hit("3", 2.0, ""));
		hits.add(new Hit("4", 2.3, ""));
		hits.add(new Hit("5", 1.0, ""));
		
		Collections.sort(hits, new Comparator<Hit>() {
			@Override
			public int compare(Hit o1, Hit o2) {
				if (o1.score == o2.score) {
					return o2.id.compareTo(o1.id);
				}
				return Double.compare(o2.score, o1.score);
			}
		});
		
		System.out.println(hits);

	}

}
