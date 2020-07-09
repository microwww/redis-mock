package com.github.microwww.redis.protocal.operation;

import com.github.microwww.redis.protocal.RedisRequest;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;

public class SearchAPI {

    @Test
    public void run() throws IOException {
        String file = SearchAPI.class.getResource("/").getFile();
        String pck = this.getClass().getPackage().getName();
        Path path = FileSystems.getDefault().getPath(new File(file).getParentFile().getParent(), "src", "main", "java", pck.replace('.', '/'));
        File opt = path.toFile();
        System.out.println(path.toFile().getCanonicalPath());
        String[] list = opt.list();
        Arrays.stream(list).filter(e -> e.length() > 5).map(e -> {
            return pck + "." + e.substring(0, e.length() - 5);
        }).map(e -> {
            try {
                return Class.forName(e);
            } catch (ClassNotFoundException ex) {
                throw new RuntimeException(ex);
            }
        }).sorted(Comparator.comparing(Class::getName)).forEach(e -> {
            System.out.println();
            System.out.println();
            System.out.println(e.getSimpleName());
            System.out.print("  ");
            Arrays.stream(e.getMethods())
                    .filter(m -> m.getParameterCount() == 1)
                    .filter(m -> {
                        return (m.getParameterTypes()[0].equals(RedisRequest.class));
                    }).map(Method::getName)
                    .sorted()
                    .forEach(m -> {
                        System.out.print(m.toUpperCase());
                        System.out.print(", ");
                    });
        });
    }
}
