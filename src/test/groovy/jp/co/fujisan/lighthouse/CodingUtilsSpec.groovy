package jp.co.fujisan.lighthouse;

import java.io.IOException;
import java.util.Random;
import static org.junit.Assert.*
import org.gmock.WithGMock
import org.junit.runner.RunWith

import org.spockframework.runtime.Sputnik
import spock.lang.*

@WithGMock
@RunWith(Sputnik)
class CodingUtilsSpec extends Specification{
    Random random = new Random()
    def uncompressed = "someranfdomevaluestringsarenotspecifiedyetbutifeedyousomestringswahtever111020349521756478625829756927264736875687653"
    
    def "compress でDEFLATEにより圧縮された文字列は と DEFLATEでextract すると同じ文字列になる"(){
        when:
        def compressed = CodingUtils.compress(uncompressed, CodingUtils.CODE_COMPRESSION_TYPE_DEFLATE);
        
        then:
        compressed
        compressed != uncompressed
        uncompressed == CodingUtils.extract(compressed, CodingUtils.CODE_COMPRESSION_TYPE_DEFLATE)
    }
    
    def "compress で CODE_COMPRESSION_TYPE_GZIP により圧縮された文字列は と CODE_COMPRESSION_TYPE_GZIP でextract すると同じ文字列になる"(){
        when:
        def compressed = CodingUtils.compress(uncompressed, CodingUtils.CODE_COMPRESSION_TYPE_GZIP);
        
        then:
        compressed
        compressed != uncompressed
        uncompressed == CodingUtils.extract(compressed, CodingUtils.CODE_COMPRESSION_TYPE_GZIP)
    }
    
    def "compress で 不正なフォーマット により圧縮しようとするとExceptionが発生する"(){
        when:
        def compressed = CodingUtils.compress(uncompressed, -1);
        
        then:
        thrown(Exception)
    }
    
    def "compress で DEFLATE により圧縮された文字列は と CODE_COMPRESSION_TYPE_GZIP でextract するとExceptionが発生する"(){
        when:
        def compressed = CodingUtils.compress(uncompressed, CodingUtils.CODE_COMPRESSION_TYPE_DEFLATE);
        CodingUtils.extract(compressed, CodingUtils.CODE_COMPRESSION_TYPE_GZIP)
        
        then:
        thrown(Exception)
    }
}
