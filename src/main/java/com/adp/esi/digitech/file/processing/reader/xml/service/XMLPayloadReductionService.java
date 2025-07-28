package com.adp.esi.digitech.file.processing.reader.xml.service;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.adp.esi.digitech.file.processing.ds.config.model.FileMetaData;
import com.adp.esi.digitech.file.processing.exception.ConfigurationException;
import com.adp.esi.digitech.file.processing.model.RequestContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

@Service
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class XMLPayloadReductionService {

	private final ObjectMapper objectMapper;

	private final RequestContext requestContext;

	@Value("${large.request.file.path}")
	protected String largeRequestFilePath;

	@Autowired(required = true)
	public XMLPayloadReductionService(ObjectMapper objectMapper, RequestContext requestContext) {
		this.objectMapper = objectMapper;
		this.requestContext = requestContext;
	}

	// Allowed attributes for XML elements during filtering
	private static final Set<String> ALLOWED_ATTRS = Set.of("isUpdated", "isAdded", "isDeleted");

	/**
	 * Context class to maintain state for XML tag processing Contains tag
	 * information, attributes, and buffered content
	 */
	static class TagContext {
		final String tagName;
		final boolean hasAllowedAttribute;
		final boolean hasIsAddedAttribute;
		boolean hasChildren = false;
		final StringWriter buffer;
		final XMLStreamWriter writer;

		TagContext(String tagName, boolean hasAllowedAttribute, boolean hasIsAddedAttribute) throws XMLStreamException {
			this.tagName = tagName;
			this.hasAllowedAttribute = hasAllowedAttribute;
			this.hasIsAddedAttribute = hasIsAddedAttribute;
			this.buffer = new StringWriter(256);
			this.writer = XMLOutputFactory.newInstance().createXMLStreamWriter(buffer);
		}
	}

	public InputStream processXML(InputStream inputStream, FileMetaData fileMetaData) {
		try {
			// Retrieve configuration from data studio service

			Set<String> validTagNames = extractTagNamesFromTemplate(objectMapper.readTree(fileMetaData.getTemplate()));

			log.info("XMLPayloadReductionService -> processXML()- Extracted {} valid tag names for filtering: {}",
					validTagNames.size(), validTagNames);

			// Filter XML with the extracted tag names
			var resultStream = filterXml(inputStream, validTagNames);

			log.info("XMLPayloadReductionService -> processXML()- Successfully completed XML processing.");
			return resultStream;

		} catch (Exception e) {
			log.error("XMLPayloadReductionService-> processXML()- Error while processing XML for input: {} - Error: {}",
					fileMetaData.getSourceKey(), e.getMessage(), e);
			throw new RuntimeException("Failed to process XML: " + fileMetaData.getSourceKey(), e);
		}

	}

	public Path processXML(String inputPath, FileMetaData fileMetaData) {
		try (FileInputStream fis = new FileInputStream(inputPath)) {
			log.info("XMLPayloadReductionService-> processXML()- Starting XML processing: input={}, unique Id={}",
					inputPath, requestContext.getUniqueId());

			// Retrieve configuration from data studio service
			Set<String> validTagNames = getTags(objectMapper.convertValue(fileMetaData.getTemplate(), JsonNode.class));

			log.info("XMLPayloadReductionService-> processXML()- Extracted {} valid tag names for filtering: {}",
					validTagNames.size(), validTagNames);

			// Filter XML with the extracted tag names
			var outputPath = Paths.get(largeRequestFilePath + requestContext.getRequestUuid() + "/"
					+ fileMetaData.getSourceKey() + "filtered_data.xml");

			filterXml(fis, outputPath, validTagNames);

			log.info(
					"XMLPayloadReductionService -> processXML()- Successfully completed XML processing. Output written to: {}",
					outputPath);
			return outputPath;

		} catch (Exception e) {
			log.error("XMLPayloadReductionService->processXML()- Error while processing XML for input: {} - Error: {}",
					inputPath, e.getMessage(), e);
			throw new RuntimeException("Failed to process XML: " + inputPath, e);
		}

	}

	private Set<String> getTags(JsonNode data) {
		return Optional.ofNullable(data).map(template -> {
			JsonNode templateNode = objectMapper.convertValue(template, JsonNode.class);
			return extractTagNamesFromTemplate(templateNode);
		}).orElseThrow(() -> {
			var configurationException = new ConfigurationException("");
			configurationException.setRequestContext(requestContext);
			return configurationException;
		});
	}

	private InputStream filterXml(InputStream fis, Set<String> validTagNames) throws Exception {
		XMLInputFactory inputFactory = XMLInputFactory.newInstance();
		XMLOutputFactory outputFactory = XMLOutputFactory.newInstance();

		log.info("XMLPayloadReductionService-> filterXml() - Starting in-memory XML filtering.....");

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		XMLStreamReader reader = inputFactory.createXMLStreamReader(fis);
		XMLStreamWriter writer = outputFactory.createXMLStreamWriter(baos, "UTF-8");

		filterXmlCore(reader, writer, validTagNames);

		log.info(
				"XMLPayloadReductionService-> filterXml() - In-memory filtering completed. Returning filtered InpuStream");

		return new ByteArrayInputStream(baos.toByteArray());
	}

	private void filterXml(InputStream fis, Path outputPath, Set<String> validTagNames) throws Exception {
		log.info("XMLPayloadReductionService-> filterXml() - Starting XML filtering process: Input{} -> Output -> {}",
				"<stream>", outputPath);

		XMLInputFactory inputFactory = XMLInputFactory.newInstance();
		XMLOutputFactory outputFactory = XMLOutputFactory.newInstance();

		try (FileOutputStream fos = new FileOutputStream(outputPath.toFile())) {

			XMLStreamReader reader = inputFactory.createXMLStreamReader(fis);
			XMLStreamWriter writer = outputFactory.createXMLStreamWriter(fos, "UTF-8");

			filterXmlCore(reader, writer, validTagNames);
			log.info("XMLPayloadReductionService-> filterXml() - XML filtering completed, Output wrtten to: {}",
					outputPath);

		} catch (Exception e) {
			log.error("XMLPayloadReductionService-> filterXml() - Error occurred while filtering XML: {}",
					e.getMessage(), e);
			throw e;
		}
	}

	/**
	 * Core XML filtering method based on your specific requirements:
	 *
	 * FILTERING RULES: 1. Always keep Effective_Change section structure 2. Keep
	 * ALL elements outside Effective_Change (like Metadata sections) 3. Inside
	 * Effective_Change: Keep elements that have allowed attributes (isAdded,
	 * isUpdated, isDeleted) 4. Inside Effective_Change: Keep ONLY the direct
	 * children that are simple data elements 5. Inside Effective_Change: Filter out
	 * nested structures that don't have allowed attributes 6. Must be a valid tag
	 * name from template
	 */
	private void filterXmlCore(XMLStreamReader reader, XMLStreamWriter writer, Set<String> validTagNames)
			throws XMLStreamException {

		log.info("XMLPayloadReductionService-> filterXmlCore() - XML filtering started with validtagNames");
		Deque<TagContext> stack = new ArrayDeque<>();
		boolean insideEffectiveChange = false;
		int effectiveChangeDepth = -1;
		boolean rootWritten = false;
		String rootLocalName = null;
		String rootPrefix = null;
		String rootNamespace = null;

		while (reader.hasNext()) {
			int event = reader.next();

			switch (event) {
			case XMLStreamConstants.START_ELEMENT -> {
				String tagName = reader.getLocalName();

				// Capture root info dynamically
				if (stack.isEmpty()) {
					rootLocalName = tagName;
					rootPrefix = reader.getPrefix();
					rootNamespace = reader.getNamespaceURI();
				}

				if ("Effective_Change".equals(tagName)) {
					insideEffectiveChange = true;
					effectiveChangeDepth = stack.size();
				}

				boolean hasAttr = hasAllowedAttribute(reader);
				boolean hasIsAddedAttr = hasIsAddedAttribute(reader);
				TagContext ctx = new TagContext(tagName, hasAttr, hasIsAddedAttr);
				stack.push(ctx);

				// Mark parent as having children
				stack.stream().skip(1).findFirst().ifPresent(parent -> parent.hasChildren = true);

				writeStartElement(reader, ctx.writer);
			}

			case XMLStreamConstants.CHARACTERS -> {
				if (!reader.isWhiteSpace() && !stack.isEmpty()) {
					stack.peek().writer.writeCharacters(reader.getText());
				}
			}

			case XMLStreamConstants.END_ELEMENT -> {
				String tagName = reader.getLocalName();
				if (stack.isEmpty())
					break;

				TagContext ctx = stack.pop();
				ctx.writer.writeEndElement();
				ctx.writer.flush();

				String content = ctx.buffer.toString();

				// Reset if closing Effective_Change
				if (stack.size() == effectiveChangeDepth && insideEffectiveChange) {
					insideEffectiveChange = false;
					effectiveChangeDepth = -1;
				}

				// Determine if the tag should be kept
				boolean keep;

				if (hasParentWithIsAdded(stack)) {
					keep = true;
				} else {
					keep = validTagNames.contains(tagName)
							&& (!insideEffectiveChange || ctx.hasAllowedAttribute || !ctx.hasChildren);
				}
				if (stack.isEmpty()) {
					if (!rootWritten) {
						// Write the root start tag dynamically
						writer.writeStartDocument("UTF-8", "1.0");
						rootWritten = true;
					}
					writer.writeCharacters("\n");
					writeXmlFromString(content, writer, rootLocalName);
				} else {
					if (keep) {
						writeXmlFromString(content, stack.peek().writer, rootLocalName);
					}
				}
			}
			}
		}
		// root
		writer.writeEndDocument();
		writer.flush();
		writer.close();

		log.info("XMLPayloadReductionService-> filterXmlCore() - XML filtering Completed");

	}

	private static boolean hasIsAddedAttribute(XMLStreamReader reader) {
		for (int i = 0; i < reader.getAttributeCount(); i++) {
			if ("isAdded".equalsIgnoreCase(reader.getAttributeLocalName(i))) {
				return true;
			}
		}
		return false;
	}

	private static boolean hasParentWithIsAdded(Deque<TagContext> stack) {
		return stack.stream().anyMatch(ctx -> ctx.hasIsAddedAttribute);
	}

	private boolean hasAllowedAttribute(XMLStreamReader reader) {
		return IntStream.range(0, reader.getAttributeCount()).mapToObj(reader::getAttributeLocalName)
				.anyMatch(ALLOWED_ATTRS::contains);
	}

	/**
	 * Writes XML start element with all namespaces and attributes Uses Stream API
	 * for processing namespaces and attributes
	 *
	 * @param reader XMLStreamReader source
	 * @param writer XMLStreamWriter destination
	 * @throws XMLStreamException if writing fails
	 */

	private static void writeStartElement(XMLStreamReader reader, XMLStreamWriter writer) throws XMLStreamException {
		String prefix = reader.getPrefix();
		String ns = reader.getNamespaceURI();
		String localName = reader.getLocalName();

		log.debug(
				"XMLPayloadReductionService-> writeStartElement() -Writing start element  for tag: {}, prefix:{}, namespace: {}",
				localName, prefix, ns);

		if (ns != null && !ns.isEmpty()) {
			writer.writeStartElement(prefix, localName, ns);
		} else {
			writer.writeStartElement(localName);
		}

		for (int i = 0; i < reader.getNamespaceCount(); i++) {
			String nsPrefix = reader.getNamespacePrefix(i);
			String nsURI = reader.getNamespaceURI(i);
			if (nsPrefix != null) {
				writer.writeNamespace(nsPrefix, nsURI);
			} else {
				writer.writeDefaultNamespace(nsURI);
			}
		}

		for (int i = 0; i < reader.getAttributeCount(); i++) {
			String attrPrefix = reader.getAttributePrefix(i);
			String attrNs = reader.getAttributeNamespace(i);
			String attrLocal = reader.getAttributeLocalName(i);
			String value = reader.getAttributeValue(i);

			if (attrNs != null && !attrNs.isEmpty()) {
				writer.writeAttribute(attrPrefix, attrNs, attrLocal, value);
			} else {
				writer.writeAttribute(attrLocal, value);
			}
		}

		log.debug("XMLPayloadReductionService-> writeStartElement() -Finished Writing start element  for tag: {}",
				localName, prefix, ns);
	}

	/**
	 * Parses a string fragment of XML and writes it using a writer, skipping the
	 * provided root tag if necessary.
	 *
	 * @param xmlFragment Fragment string to parse and write
	 * @param writer      XML writer to write to
	 * @param rootToSkip  Tag name to ignore (e.g. outer root element)
	 */
	private static void writeXmlFromString(String xmlFragment, XMLStreamWriter writer, String rootToSkip)
			throws XMLStreamException {
		XMLInputFactory factory = XMLInputFactory.newInstance();

		//log.info("XMLPayloadReductionService-> writeXmlFromString() - Start writing the XML fragment");
		String wrapped = "<root xmlns:peci=\"urn:com.workday/peci\" xmlns:ptdf=\"urn:com.workday/peci/tdf\">"
				+ xmlFragment + "</root>";

		try (StringReader stringReader = new StringReader(wrapped)) {
			XMLStreamReader reader = factory.createXMLStreamReader(stringReader);

			while (reader.hasNext()) {
				int event = reader.next();

				switch (event) {
				case XMLStreamConstants.START_ELEMENT -> {
					String local = reader.getLocalName();
					if ("root".equals(local))
						break;

					String prefix = reader.getPrefix();
					String ns = reader.getNamespaceURI();

					if (ns != null && !ns.isEmpty()) {
						writer.writeStartElement(prefix, local, ns);
					} else {
						writer.writeStartElement(local);
					}

					for (int i = 0; i < reader.getNamespaceCount(); i++) {
						String nsPrefix = reader.getNamespacePrefix(i);
						String nsURI = reader.getNamespaceURI(i);
						if (nsPrefix != null) {
							writer.writeNamespace(nsPrefix, nsURI);
						} else {
							writer.writeDefaultNamespace(nsURI);
						}
					}

					for (int i = 0; i < reader.getAttributeCount(); i++) {
						String attrPrefix = reader.getAttributePrefix(i);
						String attrNs = reader.getAttributeNamespace(i);
						String attrLocal = reader.getAttributeLocalName(i);
						String attrValue = reader.getAttributeValue(i);

						if (attrNs != null && !attrNs.isEmpty()) {
							writer.writeAttribute(attrPrefix, attrNs, attrLocal, attrValue);
						} else {
							writer.writeAttribute(attrLocal, attrValue);
						}
					}
				}
				case XMLStreamConstants.CHARACTERS -> writer.writeCharacters(reader.getText());
				case XMLStreamConstants.CDATA -> writer.writeCData(reader.getText());
				case XMLStreamConstants.COMMENT -> writer.writeComment(reader.getText());
				case XMLStreamConstants.END_ELEMENT -> {
					String local = reader.getLocalName();
					if (!"root".equals(local) && !local.equals(rootToSkip)) {
						writer.writeEndElement();
					}
				}
				}
			}

			reader.close();
			log.debug(
					"XMLPayloadReductionService-> writeXmlFromString() - Successfully completed writing XML fragment");
		} catch (XMLStreamException e) {
			log.error("XMLPayloadReductionService-> writeXmlFromString() - Error while writing the xml from String");
			throw new XMLStreamException("Error while writing XML from string", e);
		}
	}

	/**
	 * Extracts tag names from JSON template using recursive traversal
	 *
	 * @param node JSON node containing template structure
	 * @return Set of extracted tag names
	 */
	private Set<String> extractTagNamesFromTemplate(JsonNode node) {
		Set<String> tagNames = new HashSet<>();
		collectTagNames(node, tagNames);
		log.debug("XMLPayloadReductionService-> Extracted {} tag names from template", tagNames.size());
		return tagNames;
	}

	/**
	 * Recursively collects tag names from JSON template structure Uses Stream API
	 * for processing child nodes
	 *
	 * @param node     JSON node to process
	 * @param tagNames Set to collect tag names
	 */
	private void collectTagNames(JsonNode node, Set<String> tagNames) {
		// Process current node fields using Stream API
		node.fieldNames().forEachRemaining(field -> {
			if ("name".equals(field.trim())) {
				String tagName = node.get(field).asText();
				tagNames.add(tagName);
				log.trace("XMLPayloadReductionService-> Added tag name: {}", tagName);
			}
		});

		// Process child nodes recursively using Stream API
		node.fieldNames().forEachRemaining(field -> {
			if ("child".equals(field.trim())) {
				JsonNode childNode = node.get(field);
				if (childNode != null && childNode.isArray()) {
					childNode.forEach(child -> collectTagNames(child, tagNames));
				}
			}
		});
	}
}
