package ru.mailarchiva.orientdb.snappy;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.compression.OCompressionFactory;
import com.orientechnologies.orient.core.compression.impl.OAbstractCompression;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;
import org.xerial.snappy.Snappy;

import java.io.IOException;

/**
 * Created by Valentin Popov valentin@archiva.ru on 10.01.2020.
 */
public class OSnappyPlugin extends OServerPluginAbstract {

    public String getName() {
        return "snappy-plugin";
    }

    @Override
    public void startup() {
        super.startup();
        OCompressionFactory.INSTANCE.register(new SnappyCompression());
        OLogManager.instance().info(this, "snappy compression registered");
    }

    @Override
    public void config(OServer oServer, OServerParameterConfiguration[] iParams) {
        super.config(oServer, iParams);
    }

    private static class SnappyCompression extends OAbstractCompression {
        public static final String NAME = "snappy";

        @Override
        public byte[] compress(byte[] content, final int offset, final int length) {
            try {
                final byte[] buf = new byte[Snappy.maxCompressedLength(length)];
                final int compressedByteSize = Snappy.rawCompress(content, offset, length, buf, 0);
                final byte[] result = new byte[compressedByteSize];
                System.arraycopy(buf, 0, result, 0, compressedByteSize);
                return result;
            } catch (IOException e) {
                throw OException.wrapException(new ODatabaseException("Error during data compression"), e);
            }
        }

        @Override
        public byte[] uncompress(byte[] content, final int offset, final int length) {
            try {
                byte[] result = new byte[Snappy.uncompressedLength(content, offset, length)];
                int byteSize = Snappy.uncompress(content, offset, length, result, 0);
                return result;

            } catch (IOException e) {
                throw OException.wrapException(new ODatabaseException("Error during data decompression"), e);
            }
        }

        @Override
        public String name() {
            return NAME;
        }
    }

}
