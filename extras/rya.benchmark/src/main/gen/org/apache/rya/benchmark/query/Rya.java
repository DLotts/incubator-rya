/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
//
// This file was generated by the JavaTM Architecture for XML Binding(JAXB) Reference Implementation, v2.2.11 
// See <a href="http://java.sun.com/xml/jaxb">http://java.sun.com/xml/jaxb</a> 
// Any modifications to this file will be lost upon recompilation of the source schema. 
// Generated on: 2017.04.25 at 11:58:19 AM EDT 
//


package org.apache.rya.benchmark.query;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for Rya complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="Rya"&gt;
 *   &lt;complexContent&gt;
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *       &lt;sequence&gt;
 *         &lt;element name="ryaInstanceName" type="{http://www.w3.org/2001/XMLSchema}string"/&gt;
 *         &lt;element name="accumulo"&gt;
 *           &lt;complexType&gt;
 *             &lt;complexContent&gt;
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *                 &lt;sequence&gt;
 *                   &lt;element name="username" type="{http://www.w3.org/2001/XMLSchema}string"/&gt;
 *                   &lt;element name="password" type="{http://www.w3.org/2001/XMLSchema}string"/&gt;
 *                   &lt;element name="zookeepers" type="{http://www.w3.org/2001/XMLSchema}string"/&gt;
 *                   &lt;element name="instanceName" type="{http://www.w3.org/2001/XMLSchema}string"/&gt;
 *                 &lt;/sequence&gt;
 *               &lt;/restriction&gt;
 *             &lt;/complexContent&gt;
 *           &lt;/complexType&gt;
 *         &lt;/element&gt;
 *         &lt;element name="secondaryIndexing"&gt;
 *           &lt;complexType&gt;
 *             &lt;complexContent&gt;
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *                 &lt;sequence&gt;
 *                   &lt;element name="usePCJ" type="{http://www.w3.org/2001/XMLSchema}boolean"/&gt;
 *                 &lt;/sequence&gt;
 *               &lt;/restriction&gt;
 *             &lt;/complexContent&gt;
 *           &lt;/complexType&gt;
 *         &lt;/element&gt;
 *       &lt;/sequence&gt;
 *     &lt;/restriction&gt;
 *   &lt;/complexContent&gt;
 * &lt;/complexType&gt;
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "Rya", propOrder = {
    "ryaInstanceName",
    "accumulo",
    "secondaryIndexing"
})
public class Rya {

    @XmlElement(required = true)
    protected String ryaInstanceName;
    @XmlElement(required = true)
    protected Rya.Accumulo accumulo;
    @XmlElement(required = true)
    protected Rya.SecondaryIndexing secondaryIndexing;

    /**
     * Gets the value of the ryaInstanceName property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getRyaInstanceName() {
        return ryaInstanceName;
    }

    /**
     * Sets the value of the ryaInstanceName property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setRyaInstanceName(String value) {
        this.ryaInstanceName = value;
    }

    /**
     * Gets the value of the accumulo property.
     * 
     * @return
     *     possible object is
     *     {@link Rya.Accumulo }
     *     
     */
    public Rya.Accumulo getAccumulo() {
        return accumulo;
    }

    /**
     * Sets the value of the accumulo property.
     * 
     * @param value
     *     allowed object is
     *     {@link Rya.Accumulo }
     *     
     */
    public void setAccumulo(Rya.Accumulo value) {
        this.accumulo = value;
    }

    /**
     * Gets the value of the secondaryIndexing property.
     * 
     * @return
     *     possible object is
     *     {@link Rya.SecondaryIndexing }
     *     
     */
    public Rya.SecondaryIndexing getSecondaryIndexing() {
        return secondaryIndexing;
    }

    /**
     * Sets the value of the secondaryIndexing property.
     * 
     * @param value
     *     allowed object is
     *     {@link Rya.SecondaryIndexing }
     *     
     */
    public void setSecondaryIndexing(Rya.SecondaryIndexing value) {
        this.secondaryIndexing = value;
    }


    /**
     * <p>Java class for anonymous complex type.
     * 
     * <p>The following schema fragment specifies the expected content contained within this class.
     * 
     * <pre>
     * &lt;complexType&gt;
     *   &lt;complexContent&gt;
     *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
     *       &lt;sequence&gt;
     *         &lt;element name="username" type="{http://www.w3.org/2001/XMLSchema}string"/&gt;
     *         &lt;element name="password" type="{http://www.w3.org/2001/XMLSchema}string"/&gt;
     *         &lt;element name="zookeepers" type="{http://www.w3.org/2001/XMLSchema}string"/&gt;
     *         &lt;element name="instanceName" type="{http://www.w3.org/2001/XMLSchema}string"/&gt;
     *       &lt;/sequence&gt;
     *     &lt;/restriction&gt;
     *   &lt;/complexContent&gt;
     * &lt;/complexType&gt;
     * </pre>
     * 
     * 
     */
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "", propOrder = {
        "username",
        "password",
        "zookeepers",
        "instanceName"
    })
    public static class Accumulo {

        @XmlElement(required = true)
        protected String username;
        @XmlElement(required = true)
        protected String password;
        @XmlElement(required = true)
        protected String zookeepers;
        @XmlElement(required = true)
        protected String instanceName;

        /**
         * Gets the value of the username property.
         * 
         * @return
         *     possible object is
         *     {@link String }
         *     
         */
        public String getUsername() {
            return username;
        }

        /**
         * Sets the value of the username property.
         * 
         * @param value
         *     allowed object is
         *     {@link String }
         *     
         */
        public void setUsername(String value) {
            this.username = value;
        }

        /**
         * Gets the value of the password property.
         * 
         * @return
         *     possible object is
         *     {@link String }
         *     
         */
        public String getPassword() {
            return password;
        }

        /**
         * Sets the value of the password property.
         * 
         * @param value
         *     allowed object is
         *     {@link String }
         *     
         */
        public void setPassword(String value) {
            this.password = value;
        }

        /**
         * Gets the value of the zookeepers property.
         * 
         * @return
         *     possible object is
         *     {@link String }
         *     
         */
        public String getZookeepers() {
            return zookeepers;
        }

        /**
         * Sets the value of the zookeepers property.
         * 
         * @param value
         *     allowed object is
         *     {@link String }
         *     
         */
        public void setZookeepers(String value) {
            this.zookeepers = value;
        }

        /**
         * Gets the value of the instanceName property.
         * 
         * @return
         *     possible object is
         *     {@link String }
         *     
         */
        public String getInstanceName() {
            return instanceName;
        }

        /**
         * Sets the value of the instanceName property.
         * 
         * @param value
         *     allowed object is
         *     {@link String }
         *     
         */
        public void setInstanceName(String value) {
            this.instanceName = value;
        }

    }


    /**
     * <p>Java class for anonymous complex type.
     * 
     * <p>The following schema fragment specifies the expected content contained within this class.
     * 
     * <pre>
     * &lt;complexType&gt;
     *   &lt;complexContent&gt;
     *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
     *       &lt;sequence&gt;
     *         &lt;element name="usePCJ" type="{http://www.w3.org/2001/XMLSchema}boolean"/&gt;
     *       &lt;/sequence&gt;
     *     &lt;/restriction&gt;
     *   &lt;/complexContent&gt;
     * &lt;/complexType&gt;
     * </pre>
     * 
     * 
     */
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "", propOrder = {
        "usePCJ"
    })
    public static class SecondaryIndexing {

        protected boolean usePCJ;

        /**
         * Gets the value of the usePCJ property.
         * 
         */
        public boolean isUsePCJ() {
            return usePCJ;
        }

        /**
         * Sets the value of the usePCJ property.
         * 
         */
        public void setUsePCJ(boolean value) {
            this.usePCJ = value;
        }

    }

}
